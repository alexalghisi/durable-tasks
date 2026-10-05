import { RedisQueue, queueKey } from "./queue/RedisQueue.ts";
import type { RunStore } from "./store/RunStore.ts";
import type { PayloadOf, TaskDefinition } from "./task.ts";
import { recordFailure, tracer } from "./tracing.ts";
import type { Run, RunEvent } from "./types.ts";

export interface TriggerOptions {
    /** Triggering again with the same key returns the first run instead of creating a new one. */
    readonly idempotencyKey?: string;
    /** Limits concurrency per key (for example per customer) instead of per queue. */
    readonly concurrencyKey?: string;
    readonly delayMs?: number;
}

export interface Triggered {
    readonly id: string;
    /** False when an earlier run with the same idempotency key was returned. */
    readonly created: boolean;
}

export class TaskClient {
    readonly #store: RunStore;
    readonly #queue: RedisQueue;

    constructor(store: RunStore, queue: RedisQueue) {
        this.#store = store;
        this.#queue = queue;
    }

    /**
     * Validates the payload, stores the run in Postgres and then queues it in Redis.
     * If Redis is unavailable after the insert, the run stays queued in Postgres and a
     * worker's recovery loop puts it back in Redis.
     */
    async trigger<T extends TaskDefinition>(task: T, payload: PayloadOf<T>, options: TriggerOptions = {}): Promise<Triggered> {
        return tracer().startActiveSpan(`trigger ${task.id}`, async (span) => {
            try {
                const parsed = task.parsePayload(payload);
                const { traceId, spanId } = span.spanContext();
                const sampled = span.isRecording();
                const { run, created } = await this.#store.create({
                    taskId: task.id,
                    payload: parsed,
                    queue: task.queue.name,
                    concurrencyKey: options.concurrencyKey ?? null,
                    idempotencyKey: options.idempotencyKey ?? null,
                    runAt: new Date(Date.now() + (options.delayMs ?? 0)),
                    traceId: sampled ? traceId : null,
                    spanId: sampled ? spanId : null,
                });
                span.setAttributes({ "run.id": run.id, "run.created": created, "task.id": task.id });
                if (created) await this.#queue.enqueue(queueKey(run.queue, run.concurrencyKey), run.id, run.runAt);
                return { id: run.id, created };
            } catch (error) {
                recordFailure(span, error);
                throw error;
            } finally {
                span.end();
            }
        });
    }

    retrieve(id: string): Promise<Run | undefined> {
        return this.#store.get(id);
    }

    events(id: string): Promise<RunEvent[]> {
        return this.#store.events(id);
    }

    /** Cancels a run. A queued run never starts; an executing one is aborted at its next heartbeat. */
    async cancel(id: string): Promise<Run | undefined> {
        const run = await this.#store.cancel(id);
        if (run) await this.#queue.remove(queueKey(run.queue, run.concurrencyKey), run.id);
        return run;
    }
}
