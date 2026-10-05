import { Redis } from "ioredis";
import pg from "pg";
import { RunStore } from "../src/store/RunStore.ts";

export const DATABASE_URL = process.env.DURABLE_TASKS_TEST_DATABASE_URL;
export const REDIS_URL = process.env.DURABLE_TASKS_TEST_REDIS_URL;

/** Integration tests need real services; they are skipped when these aren't configured. */
export const hasServices = DATABASE_URL !== undefined && REDIS_URL !== undefined;

export async function freshDatabase(): Promise<pg.Pool> {
    const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 20 });
    await RunStore.migrate(pool);
    await pool.query("TRUNCATE runs, run_events");
    return pool;
}

export async function freshRedis(): Promise<Redis> {
    const redis = new Redis(REDIS_URL ?? "redis://localhost:6379", { maxRetriesPerRequest: 2 });
    await redis.flushdb();
    return redis;
}

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function eventually<T>(check: () => Promise<T | undefined | false>, timeoutMs = 10_000, everyMs = 25): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
        const value = await check();
        if (value) return value;
        if (Date.now() > deadline) throw new Error("condition not met in time");
        await sleep(everyMs);
    }
}
