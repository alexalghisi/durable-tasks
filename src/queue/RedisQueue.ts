import type { Redis } from "ioredis";

/**
 * A queue is identified by its name plus an optional concurrency key, so
 * `concurrencyLimit` can apply per tenant ("1 sync per customer") instead of globally.
 */
export function queueKey(name: string, concurrencyKey: string | null): string {
    return concurrencyKey === null ? name : `${name}:${concurrencyKey}`;
}

// KEYS[1] ready zset (score = due time), KEYS[2] running zset (score = lease expiry), KEYS[3] active set
// ARGV[1] now, ARGV[2] limit, ARGV[3] lease expiry, ARGV[4] queue key
// Checking the limit and taking a run happen in one script, so no two workers can both
// take the last free slot.
const DEQUEUE = `
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[2]) then return false end
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
if #due == 0 then
  if redis.call('ZCARD', KEYS[1]) == 0 and redis.call('ZCARD', KEYS[2]) == 0 then redis.call('SREM', KEYS[3], ARGV[4]) end
  return false
end
redis.call('ZREM', KEYS[1], due[1])
redis.call('ZADD', KEYS[2], ARGV[3], due[1])
return due[1]
`;

export interface Dequeued {
    readonly queue: string;
    readonly runId: string;
}

/**
 * Redis holds what is waiting and what is running per queue. It is fast and atomic,
 * but not the source of truth: everything here can be rebuilt from Postgres.
 */
export class RedisQueue {
    readonly #redis: Redis;
    readonly #prefix: string;

    constructor(redis: Redis, prefix = "dt") {
        this.#redis = redis;
        this.#prefix = prefix;
    }

    #ready(queue: string) {
        return `${this.#prefix}:q:{${queue}}:ready`;
    }

    #running(queue: string) {
        return `${this.#prefix}:q:{${queue}}:running`;
    }

    get #active() {
        return `${this.#prefix}:active`;
    }

    async enqueue(queue: string, runId: string, runAt: Date): Promise<void> {
        await this.#redis.multi().zadd(this.#ready(queue), runAt.getTime(), runId).sadd(this.#active, queue).exec();
    }

    /** Takes the next due run of a queue if it has a free concurrency slot. */
    async dequeue(queue: string, limit: number, leaseMs: number, now = Date.now()): Promise<string | undefined> {
        const result = await this.#redis.eval(DEQUEUE, 3, this.#ready(queue), this.#running(queue), this.#active, now, limit, now + leaseMs, queue);
        return typeof result === "string" ? result : undefined;
    }

    async extend(queue: string, runId: string, leaseMs: number): Promise<void> {
        await this.#redis.zadd(this.#running(queue), "XX", Date.now() + leaseMs, runId);
    }

    /** Frees the run's concurrency slot. */
    async release(queue: string, runId: string): Promise<void> {
        await this.#redis.zrem(this.#running(queue), runId);
    }

    async remove(queue: string, runId: string): Promise<void> {
        await this.#redis.multi().zrem(this.#ready(queue), runId).zrem(this.#running(queue), runId).exec();
    }

    async activeQueues(): Promise<string[]> {
        return this.#redis.smembers(this.#active);
    }

    async depth(queue: string): Promise<{ ready: number; running: number }> {
        const [ready, running] = await Promise.all([this.#redis.zcard(this.#ready(queue)), this.#redis.zcard(this.#running(queue))]);
        return { ready, running };
    }
}
