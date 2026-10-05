import type { z } from "zod";
import { DEFAULT_RETRY, type RetryOptions } from "./backoff.ts";

export interface TaskContext {
    readonly runId: string;
    readonly attempt: number;
    /** Aborted when the run exceeds maxDurationMs, is cancelled, or the worker shuts down. */
    readonly signal: AbortSignal;
}

export interface QueueOptions {
    readonly name?: string;
    /** Runs of this queue executing at once, across all workers. */
    readonly concurrencyLimit?: number;
}

export interface TaskOptions<Payload, Output> {
    readonly id: string;
    readonly schema?: z.ZodType<Payload>;
    readonly retry?: Partial<RetryOptions>;
    readonly queue?: QueueOptions;
    readonly maxDurationMs?: number;
    readonly run: (payload: Payload, context: TaskContext) => Promise<Output>;
}

/** What workers see: payload and output types are erased, validation happens in `parsePayload`. */
export interface TaskDefinition {
    readonly id: string;
    readonly retry: RetryOptions;
    readonly queue: Required<QueueOptions>;
    readonly maxDurationMs: number;
    /** Validates an untyped payload; throws a readable error if it doesn't match the schema. */
    readonly parsePayload: (payload: unknown) => unknown;
    readonly execute: (payload: unknown, context: TaskContext) => Promise<unknown>;
}

/** What callers see: `trigger(task, payload)` is type-checked against the task's payload. */
export interface Task<Payload, Output> extends TaskDefinition {
    readonly types?: { readonly payload: Payload; readonly output: Output };
}

export type PayloadOf<T> = T extends Task<infer P, unknown> ? P : never;
export type OutputOf<T> = T extends Task<unknown, infer O> ? O : never;

const TASK_ID = /^[a-z0-9][a-z0-9-_.]{0,99}$/;

export const DEFAULT_CONCURRENCY = 10;
export const DEFAULT_MAX_DURATION_MS = 5 * 60_000;

export function task<Payload, Output>(options: TaskOptions<Payload, Output>): Task<Payload, Output> {
    if (!TASK_ID.test(options.id)) throw new Error(`Invalid task id "${options.id}": use lowercase letters, digits, - _ .`);
    const retry = { ...DEFAULT_RETRY, ...options.retry };
    if (retry.maxAttempts < 1) throw new Error(`Task "${options.id}": maxAttempts must be at least 1`);
    const concurrencyLimit = options.queue?.concurrencyLimit ?? DEFAULT_CONCURRENCY;
    if (!Number.isInteger(concurrencyLimit) || concurrencyLimit < 1) throw new Error(`Task "${options.id}": concurrencyLimit must be a positive integer`);
    const parsePayload = (payload: unknown): Payload => {
        if (options.schema === undefined) return payload as Payload;
        const result = options.schema.safeParse(payload);
        if (result.success) return result.data;
        const issues = result.error.issues.map((i) => `${i.path.join(".") || "payload"}: ${i.message}`).join("; ");
        throw new Error(`Invalid payload for task "${options.id}": ${issues}`);
    };
    return {
        id: options.id,
        retry,
        queue: { name: options.queue?.name ?? options.id, concurrencyLimit },
        maxDurationMs: options.maxDurationMs ?? DEFAULT_MAX_DURATION_MS,
        parsePayload,
        execute: (payload, context) => options.run(parsePayload(payload), context),
    };
}

export class TaskRegistry {
    readonly #tasks = new Map<string, TaskDefinition>();

    constructor(tasks: readonly TaskDefinition[] = []) {
        for (const t of tasks) this.register(t);
    }

    register(t: TaskDefinition): void {
        if (this.#tasks.has(t.id)) throw new Error(`Task "${t.id}" is registered twice`);
        this.#tasks.set(t.id, t);
    }

    get(id: string): TaskDefinition | undefined {
        return this.#tasks.get(id);
    }

    get all(): readonly TaskDefinition[] {
        return [...this.#tasks.values()];
    }
}
