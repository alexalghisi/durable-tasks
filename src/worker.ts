import { randomUUID } from "node:crypto";
import { retryDelay, shouldRetry } from "./backoff.ts";
import { RedisQueue, queueKey, queueName } from "./queue/RedisQueue.ts";
import type { Lease, RunStore } from "./store/RunStore.ts";
import type { TaskRegistry } from "./task.ts";

export interface WorkerOptions {
    readonly tasks: TaskRegistry;
    readonly store: RunStore;
    readonly queue: RedisQueue;
    readonly id?: string;
    /** Runs this worker executes at once. Queue limits still apply across all workers. */
    readonly concurrency?: number;
    readonly pollIntervalMs?: number;
    /** How long an attempt is owned without a heartbeat before it is considered lost. */
    readonly leaseMs?: number;
    readonly recoveryIntervalMs?: number;
    readonly random?: () => number;
    readonly onError?: (error: unknown) => void;
}

class LeaseLost extends Error {
    constructor(reason: string) {
        super(reason);
        this.name = "LeaseLost";
    }
}

const message = (error: unknown) => (error instanceof Error ? error.message : String(error));

/**
 * Pulls runs from Redis, claims them in Postgres and executes them.
 *
 * - Each attempt holds a lease that heartbeats renew. If the worker dies, the lease
 *   expires, recovery puts the run back in the queue and another worker retries it.
 * - Results are written with the lease, so a worker that lost its lease (paused,
 *   partitioned, or presumed dead) can't overwrite the outcome of a newer attempt.
 */
export class Worker {
    readonly id: string;
    readonly #o: Required<Omit<WorkerOptions, "id">>;
    readonly #inFlight = new Map<string, Promise<void>>();
    readonly #controllers = new Map<string, AbortController>();
    readonly #timers = new Set<ReturnType<typeof setInterval>>();
    #running = false;
    #killed = false;
    #cursor = 0;

    constructor(options: WorkerOptions) {
        this.id = options.id ?? `worker-${randomUUID().slice(0, 8)}`;
        this.#o = {
            concurrency: 10,
            pollIntervalMs: 100,
            leaseMs: 30_000,
            recoveryIntervalMs: 1_000,
            random: Math.random,
            onError: (error) => console.error(`[${this.id}]`, error),
            ...options,
        };
    }

    get busy(): number {
        return this.#inFlight.size;
    }

    start(): void {
        if (this.#running) return;
        this.#running = true;
        this.#every(this.#o.pollIntervalMs, () => this.poll());
        this.#every(this.#o.recoveryIntervalMs, () => this.recover());
    }

    /**
     * Stops taking new runs and waits up to `graceMs` for the ones in flight. Runs still
     * going after that are aborted and scheduled for retry like any other failed attempt.
     */
    async stop(graceMs = 30_000): Promise<void> {
        this.#running = false;
        for (const timer of this.#timers) clearInterval(timer);
        this.#timers.clear();
        if (this.#killed) return;
        const drained = Promise.allSettled(this.#inFlight.values());
        let timer: ReturnType<typeof setTimeout> | undefined;
        const graceOver = new Promise<"timeout">((resolve) => (timer = setTimeout(() => resolve("timeout"), graceMs)));
        if ((await Promise.race([drained, graceOver])) === "timeout") {
            for (const controller of this.#controllers.values()) controller.abort(new Error(`Worker ${this.id} shut down during the attempt`));
            await drained;
        }
        clearTimeout(timer);
    }

    /** Simulates a crash: no more polling, heartbeats or writes. Runs in flight are left to their leases. */
    kill(): void {
        this.#killed = true;
        this.#running = false;
        for (const timer of this.#timers) clearInterval(timer);
        this.#timers.clear();
    }

    /** One pass over the active queues: starts as many runs as there are free slots. */
    async poll(): Promise<number> {
        if (this.#killed) return 0;
        const queues = await this.#o.queue.activeQueues();
        let started = 0;
        for (let i = 0; i < queues.length && this.#inFlight.size < this.#o.concurrency; i++) {
            const key = queues[(this.#cursor + i) % queues.length];
            if (key === undefined) continue;
            const limit = this.#limitFor(key);
            if (limit === undefined) continue;
            while (this.#inFlight.size < this.#o.concurrency) {
                const runId = await this.#o.queue.dequeue(key, limit, this.#o.leaseMs);
                if (runId === undefined) break;
                const execution = this.#execute(key, runId)
                    .catch((error: unknown) => this.#o.onError(error))
                    .finally(() => this.#inFlight.delete(runId));
                this.#inFlight.set(runId, execution);
                started++;
            }
        }
        this.#cursor++;
        return started;
    }

    /**
     * Requeues runs whose worker stopped heartbeating. Runs found queued in Postgres
     * but missing from Redis (for example after a Redis restart) are restored too.
     */
    async recover(): Promise<{ expired: number; restored: number }> {
        if (this.#killed) return { expired: 0, restored: 0 };
        const expired = await this.#o.store.requeueExpired();
        for (const run of expired) {
            const key = queueKey(run.queue, run.concurrencyKey);
            await this.#o.queue.release(key, run.id);
            await this.#o.queue.enqueue(key, run.id, run.runAt);
        }
        let restored = 0;
        let after = "";
        for (;;) {
            const batch = await this.#o.store.queued(500, after);
            for (const run of batch) if (await this.#o.queue.restore(queueKey(run.queue, run.concurrencyKey), run.id, run.runAt)) restored++;
            if (batch.length < 500) break;
            after = batch[batch.length - 1]?.id ?? after;
        }
        return { expired: expired.length, restored };
    }

    #limitFor(key: string): number | undefined {
        const name = queueName(key);
        return this.#o.tasks.all.find((t) => t.queue.name === name)?.queue.concurrencyLimit;
    }

    async #execute(key: string, runId: string): Promise<void> {
        const { store, queue, leaseMs } = this.#o;
        const run = await store.claim(runId, this.id, leaseMs);
        if (run === undefined) {
            // Cancelled, already finished, or being executed by another worker (in which case its slot must stay taken).
            if ((await store.get(runId))?.status !== "executing") await queue.release(key, runId);
            return;
        }
        const lease: Lease = { runId, workerId: this.id, attempt: run.attempt };
        const task = this.#o.tasks.get(run.taskId);
        if (task === undefined) {
            await store.fail(lease, `No task "${run.taskId}" is registered on worker ${this.id}`, null);
            await queue.release(key, runId);
            return;
        }
        if (run.attempt > task.retry.maxAttempts) {
            await store.fail(lease, `Gave up after ${task.retry.maxAttempts} attempts: the last one was lost with its worker`, null);
            await queue.release(key, runId);
            return;
        }

        const controller = new AbortController();
        this.#controllers.set(runId, controller);
        const timeout = setTimeout(() => controller.abort(new Error(`Exceeded maxDurationMs (${task.maxDurationMs} ms)`)), task.maxDurationMs);
        const heartbeat = setInterval(() => {
            if (this.#killed) return clearInterval(heartbeat);
            store
                .heartbeat(lease, leaseMs)
                .then(async (owned) => {
                    if (owned) await queue.extend(key, runId, leaseMs);
                    else controller.abort(new LeaseLost("Run was cancelled or its lease was taken over"));
                })
                .catch((error: unknown) => this.#o.onError(error));
        }, Math.max(10, Math.floor(leaseMs / 3)));

        try {
            const output = await abortable(task.execute(run.payload, { runId, attempt: run.attempt, signal: controller.signal }), controller.signal);
            if (this.#killed) return;
            await store.complete(lease, output);
        } catch (error) {
            if (this.#killed || controller.signal.reason instanceof LeaseLost) return;
            const reason = controller.signal.aborted ? message(controller.signal.reason) : message(error);
            if (shouldRetry(run.attempt, task.retry)) {
                const retryAt = new Date(Date.now() + retryDelay(run.attempt, task.retry, this.#o.random));
                if (await store.fail(lease, reason, retryAt)) await queue.enqueue(key, runId, retryAt);
            } else {
                await store.fail(lease, reason, null);
            }
        } finally {
            this.#controllers.delete(runId);
            clearTimeout(timeout);
            clearInterval(heartbeat);
            if (!this.#killed) await queue.release(key, runId);
        }
    }

    #every(ms: number, fn: () => Promise<unknown>): void {
        let busy = false;
        const timer = setInterval(() => {
            if (busy || !this.#running) return;
            busy = true;
            fn()
                .catch((error: unknown) => this.#o.onError(error))
                .finally(() => (busy = false));
        }, ms);
        this.#timers.add(timer);
    }
}

/** Resolves with the task's result, or rejects as soon as the signal aborts (tasks that ignore the signal can't hold a slot forever). */
function abortable<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
    if (signal.aborted) return Promise.reject(signal.reason);
    return new Promise<T>((resolve, reject) => {
        const onAbort = () => reject(signal.reason);
        signal.addEventListener("abort", onAbort, { once: true });
        work.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
    });
}
