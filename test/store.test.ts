import type pg from "pg";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { RunStore, type NewRun } from "../src/store/RunStore.ts";
import { freshDatabase, hasServices, sleep } from "./support.ts";

const newRun = (overrides: Partial<NewRun> = {}): NewRun => ({
    taskId: "send-email",
    payload: { to: "ada@example.com" },
    queue: "send-email",
    concurrencyKey: null,
    idempotencyKey: null,
    runAt: new Date(Date.now() - 1),
    traceId: null,
    spanId: null,
    ...overrides,
});

describe.skipIf(!hasServices)("RunStore (Postgres)", () => {
    let pool: pg.Pool;
    let store: RunStore;

    beforeEach(async () => {
        pool ??= await freshDatabase();
        await pool.query("TRUNCATE runs, run_events");
        store = new RunStore(pool);
    });

    afterAll(async () => {
        await pool?.end();
    });

    it("creates queued runs and records the trigger", async () => {
        const { run, created } = await store.create(newRun());
        expect(created).toBe(true);
        expect(run).toMatchObject({ status: "queued", attempt: 0, payload: { to: "ada@example.com" } });
        expect((await store.events(run.id)).map((e) => e.type)).toEqual(["triggered"]);
    });

    it("returns the existing run for a repeated idempotency key, also under concurrency", async () => {
        const results = await Promise.all(Array.from({ length: 10 }, () => store.create(newRun({ idempotencyKey: "order-42" }))));
        expect(new Set(results.map((r) => r.run.id)).size).toBe(1);
        expect(results.filter((r) => r.created)).toHaveLength(1);
        const other = await store.create(newRun({ taskId: "other-task", idempotencyKey: "order-42" }));
        expect(other.created).toBe(true);
    });

    it("lets exactly one worker claim an attempt", async () => {
        const { run } = await store.create(newRun());
        const claims = await Promise.all(Array.from({ length: 8 }, (_, i) => store.claim(run.id, `worker-${i}`, 5_000)));
        const won = claims.filter((c) => c !== undefined);
        expect(won).toHaveLength(1);
        expect(won[0]).toMatchObject({ status: "executing", attempt: 1 });
    });

    it("doesn't claim runs that aren't due yet", async () => {
        const { run } = await store.create(newRun({ runAt: new Date(Date.now() + 60_000) }));
        expect(await store.claim(run.id, "w", 5_000)).toBeUndefined();
    });

    it("only accepts results from the worker that owns the current attempt", async () => {
        const { run } = await store.create(newRun());
        await store.claim(run.id, "w1", 5_000);
        expect(await store.complete({ runId: run.id, workerId: "w2", attempt: 1 }, "nope")).toBe(false);
        expect(await store.complete({ runId: run.id, workerId: "w1", attempt: 2 }, "nope")).toBe(false);
        expect(await store.complete({ runId: run.id, workerId: "w1", attempt: 1 }, { sent: true })).toBe(true);
        expect(await store.get(run.id)).toMatchObject({ status: "completed", output: { sent: true }, lockedBy: null });
    });

    it("schedules retries and fails for good when told to", async () => {
        const { run } = await store.create(newRun());
        await store.claim(run.id, "w", 5_000);
        const retryAt = new Date(Date.now() + 30_000);
        expect(await store.fail({ runId: run.id, workerId: "w", attempt: 1 }, "SMTP 421", retryAt)).toBe(true);
        expect(await store.get(run.id)).toMatchObject({ status: "queued", error: "SMTP 421", runAt: retryAt });
        await pool.query("UPDATE runs SET run_at = now() WHERE id = $1", [run.id]);
        await store.claim(run.id, "w", 5_000);
        await store.fail({ runId: run.id, workerId: "w", attempt: 2 }, "SMTP 550", null);
        expect(await store.get(run.id)).toMatchObject({ status: "failed", attempt: 2, error: "SMTP 550" });
        expect((await store.events(run.id)).map((e) => `${e.type}#${e.attempt}`)).toEqual([
            "triggered#0",
            "started#1",
            "retry_scheduled#1",
            "started#2",
            "failed#2",
        ]);
    });

    it("requeues runs whose lease expired, and fences out the old owner", async () => {
        const { run } = await store.create(newRun());
        await store.claim(run.id, "crashed", 50);
        await sleep(120);
        const requeued = await store.requeueExpired();
        expect(requeued.map((r) => r.id)).toEqual([run.id]);
        await store.claim(run.id, "healthy", 5_000);
        expect(await store.heartbeat({ runId: run.id, workerId: "crashed", attempt: 1 }, 5_000)).toBe(false);
        expect(await store.complete({ runId: run.id, workerId: "crashed", attempt: 1 }, "late")).toBe(false);
        expect(await store.complete({ runId: run.id, workerId: "healthy", attempt: 2 }, "ok")).toBe(true);
    });

    it("cancels queued and executing runs, which stops heartbeats", async () => {
        const { run } = await store.create(newRun());
        await store.claim(run.id, "w", 5_000);
        expect(await store.cancel(run.id)).toMatchObject({ status: "cancelled" });
        expect(await store.heartbeat({ runId: run.id, workerId: "w", attempt: 1 }, 5_000)).toBe(false);
        expect(await store.cancel(run.id)).toBeUndefined();
    });
});
