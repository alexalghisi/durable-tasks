import type { Redis } from "ioredis";
import type pg from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { z } from "zod";
import { TaskClient } from "../src/client.ts";
import { RedisQueue } from "../src/queue/RedisQueue.ts";
import { RunStore } from "../src/store/RunStore.ts";
import { TaskRegistry, task, type TaskDefinition } from "../src/task.ts";
import type { Run } from "../src/types.ts";
import { Worker, type WorkerOptions } from "../src/worker.ts";
import { eventually, freshDatabase, freshRedis, hasServices, sleep } from "./support.ts";

const fastRetry = { minTimeoutMs: 20, maxTimeoutMs: 100, factor: 2, jitter: 0 };

describe.skipIf(!hasServices)("Worker", () => {
    let pool: pg.Pool;
    let redis: Redis;
    let store: RunStore;
    let queue: RedisQueue;
    let client: TaskClient;
    let workers: Worker[] = [];

    beforeAll(async () => {
        pool = await freshDatabase();
        redis = await freshRedis();
    });

    beforeEach(async () => {
        await pool.query("TRUNCATE runs, run_events");
        await redis.flushdb();
        store = new RunStore(pool);
        queue = new RedisQueue(redis);
        client = new TaskClient(store, queue);
    });

    afterEach(async () => {
        await Promise.all(workers.map((w) => w.stop(1_000)));
        workers = [];
    });

    afterAll(async () => {
        await pool.end();
        redis.disconnect();
    });

    function startWorker(tasks: readonly TaskDefinition[], options: Partial<WorkerOptions> = {}): Worker {
        const worker = new Worker({ tasks: new TaskRegistry(tasks), store, queue, pollIntervalMs: 10, recoveryIntervalMs: 50, onError: () => {}, ...options });
        workers.push(worker);
        worker.start();
        return worker;
    }

    const settled = (id: string) =>
        eventually(async () => {
            const run = await store.get(id);
            return run && ["completed", "failed", "cancelled"].includes(run.status) ? run : undefined;
        });

    it("runs a task and stores its output", async () => {
        const double = task({ id: "double", schema: z.object({ n: z.number() }), run: async ({ n }) => n * 2 });
        startWorker([double]);
        const { id } = await client.trigger(double, { n: 21 });
        expect(await settled(id)).toMatchObject({ status: "completed", output: 42, attempt: 1 });
        expect((await client.events(id)).map((e) => e.type)).toEqual(["triggered", "started", "completed"]);
    });

    it("rejects invalid payloads at trigger time", async () => {
        const typed = task({ id: "typed", schema: z.object({ email: z.email() }), run: async () => "ok" });
        await expect(client.trigger(typed, { email: "not-an-email" })).rejects.toThrow(/Invalid payload for task "typed": email/);
        expect(await store.list()).toHaveLength(0);
    });

    it("deduplicates triggers with the same idempotency key", async () => {
        let calls = 0;
        const charge = task({ id: "charge", run: async () => ++calls });
        startWorker([charge]);
        const first = await client.trigger(charge, { amount: 10 }, { idempotencyKey: "order-42" });
        const second = await client.trigger(charge, { amount: 10 }, { idempotencyKey: "order-42" });
        expect(second).toEqual({ id: first.id, created: false });
        await settled(first.id);
        await sleep(100);
        expect(calls).toBe(1);
    });

    it("retries with backoff until the task succeeds", async () => {
        let attempts = 0;
        const flaky = task({
            id: "flaky",
            retry: { ...fastRetry, maxAttempts: 4 },
            run: async () => {
                attempts++;
                if (attempts < 3) throw new Error(`503 on attempt ${attempts}`);
                return "delivered";
            },
        });
        startWorker([flaky]);
        const { id } = await client.trigger(flaky, {});
        expect(await settled(id)).toMatchObject({ status: "completed", output: "delivered", attempt: 3, error: null });
        const events = (await client.events(id)).map((e) => `${e.type}#${e.attempt}`);
        expect(events).toEqual(["triggered#0", "started#1", "retry_scheduled#1", "started#2", "retry_scheduled#2", "started#3", "completed#3"]);
    });

    it("fails for good after maxAttempts, keeping the last error", async () => {
        const broken = task({ id: "broken", retry: { ...fastRetry, maxAttempts: 2 }, run: async (_: unknown, ctx) => Promise.reject(new Error(`boom ${ctx.attempt}`)) });
        startWorker([broken]);
        const { id } = await client.trigger(broken, {});
        expect(await settled(id)).toMatchObject({ status: "failed", attempt: 2, error: "boom 2" });
    });

    it("never runs more than the queue's concurrency limit across workers", async () => {
        let current = 0;
        let peak = 0;
        const limited = task({
            id: "limited",
            queue: { concurrencyLimit: 4 },
            run: async () => {
                peak = Math.max(peak, ++current);
                await sleep(30);
                current--;
            },
        });
        for (let i = 0; i < 3; i++) startWorker([limited], { concurrency: 10 });
        const runs = await Promise.all(Array.from({ length: 30 }, () => client.trigger(limited, {})));
        await Promise.all(runs.map((r) => settled(r.id)));
        expect(peak).toBe(4);
        expect(await store.list({ status: "completed" })).toHaveLength(30);
    });

    it("applies the limit per concurrency key", async () => {
        const perCustomer = new Map<string, number>();
        let peakPerCustomer = 0;
        let current = 0;
        let peakOverall = 0;
        const sync = task({
            id: "sync",
            queue: { concurrencyLimit: 1 },
            run: async ({ customer }: { customer: string }) => {
                const now = (perCustomer.get(customer) ?? 0) + 1;
                perCustomer.set(customer, now);
                peakPerCustomer = Math.max(peakPerCustomer, now);
                peakOverall = Math.max(peakOverall, ++current);
                await sleep(25);
                perCustomer.set(customer, (perCustomer.get(customer) ?? 1) - 1);
                current--;
            },
        });
        startWorker([sync], { concurrency: 10 });
        startWorker([sync], { concurrency: 10 });
        const runs = await Promise.all(
            ["acme", "globex", "initech"].flatMap((customer) => Array.from({ length: 5 }, () => client.trigger(sync, { customer }, { concurrencyKey: customer }))),
        );
        await Promise.all(runs.map((r) => settled(r.id)));
        expect(peakPerCustomer).toBe(1);
        expect(peakOverall).toBeGreaterThan(1);
    });

    it("recovers runs from a worker that died mid-attempt, and ignores its late result", async () => {
        const started: string[] = [];
        let release!: () => void;
        const gate = new Promise<void>((resolve) => (release = resolve));
        const slow = task({
            id: "slow",
            run: async (_: unknown, ctx) => {
                started.push(`attempt ${ctx.attempt}`);
                if (ctx.attempt === 1) await gate;
                return `finished by attempt ${ctx.attempt}`;
            },
        });
        const doomed = startWorker([slow], { id: "doomed", leaseMs: 300, recoveryIntervalMs: 60_000 });
        const { id } = await client.trigger(slow, {});
        await eventually(async () => started.length === 1);
        doomed.kill();

        startWorker([slow], { id: "survivor", leaseMs: 300 });
        const run = await settled(id);
        release();
        await sleep(100);

        expect(run).toMatchObject({ status: "completed", attempt: 2, output: "finished by attempt 2" });
        expect(await store.get(id)).toMatchObject({ output: "finished by attempt 2" });
        expect((await client.events(id)).map((e) => e.type)).toEqual(["triggered", "started", "lease_expired", "started", "completed"]);
    });

    it("gives up when the last allowed attempt was lost with its worker", async () => {
        const hangs = task({ id: "hangs", retry: { maxAttempts: 1 }, run: () => new Promise<never>(() => {}) });
        const doomed = startWorker([hangs], { leaseMs: 200, recoveryIntervalMs: 60_000 });
        const { id } = await client.trigger(hangs, {});
        await eventually(async () => (await store.get(id))?.status === "executing");
        doomed.kill();
        startWorker([hangs], { leaseMs: 200 });
        expect(await settled(id)).toMatchObject({ status: "failed", error: "Gave up after 1 attempts: the last one was lost with its worker" });
    });

    it("aborts attempts that exceed maxDurationMs", async () => {
        let sawAbort = false;
        const stuck = task({
            id: "stuck",
            maxDurationMs: 100,
            retry: { maxAttempts: 1 },
            run: (_: unknown, ctx) =>
                new Promise<never>((_resolve, reject) => {
                    ctx.signal.addEventListener("abort", () => {
                        sawAbort = true;
                        reject(ctx.signal.reason);
                    });
                }),
        });
        startWorker([stuck]);
        const { id } = await client.trigger(stuck, {});
        expect(await settled(id)).toMatchObject({ status: "failed", error: "Exceeded maxDurationMs (100 ms)" });
        expect(sawAbort).toBe(true);
    });

    it("cancels an executing run and frees its slot", async () => {
        const seen: string[] = [];
        const longRunning = task({
            id: "long",
            queue: { concurrencyLimit: 1 },
            run: (payload: { name: string }, ctx) =>
                new Promise<string>((resolve) => {
                    seen.push(payload.name);
                    if (payload.name === "quick") return resolve("done");
                    ctx.signal.addEventListener("abort", () => resolve("aborted"));
                }),
        });
        startWorker([longRunning], { leaseMs: 150 });
        const blocker = await client.trigger(longRunning, { name: "blocker" });
        const next = await client.trigger(longRunning, { name: "quick" });
        await eventually(async () => seen.includes("blocker"));
        await client.cancel(blocker.id);
        expect(await settled(next.id)).toMatchObject({ status: "completed" });
        expect(await store.get(blocker.id)).toMatchObject({ status: "cancelled" });
    });

    it("on shutdown, aborts attempts that outlive the grace period so another worker retries them", async () => {
        const drain = task({
            id: "drain",
            retry: { ...fastRetry, maxAttempts: 2 },
            run: (_: unknown, ctx) => (ctx.attempt === 1 ? new Promise<string>(() => {}) : Promise.resolve("second attempt")),
        });
        const leaving = startWorker([drain]);
        const { id } = await client.trigger(drain, {});
        await eventually(async () => (await store.get(id))?.status === "executing");
        await leaving.stop(50);
        startWorker([drain]);
        expect(await settled(id)).toMatchObject({ status: "completed", attempt: 2, output: "second attempt" });
        const retry = (await client.events(id)).find((e) => e.type === "retry_scheduled");
        expect(retry?.detail).toBe(`Worker ${leaving.id} shut down during the attempt`);
    });

    it("rebuilds lost Redis queues from Postgres", async () => {
        const echo = task({ id: "echo", run: async (payload: { n: number }) => payload.n });
        const runs = await Promise.all([1, 2, 3].map((n) => client.trigger(echo, { n })));
        await redis.flushdb();
        startWorker([echo]);
        const results = await Promise.all(runs.map((r) => settled(r.id)));
        expect(results.map((r: Run) => r.output)).toEqual([1, 2, 3]);
    });

    it("waits for delayed runs", async () => {
        const echo = task({ id: "echo", run: async () => Date.now() });
        startWorker([echo]);
        const triggeredAt = Date.now();
        const { id } = await client.trigger(echo, {}, { delayMs: 300 });
        const run = await settled(id);
        expect((run.output as number) - triggeredAt).toBeGreaterThanOrEqual(300);
    });
});
