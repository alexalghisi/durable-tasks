import type { Redis } from "ioredis";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { RedisQueue, queueKey } from "../src/queue/RedisQueue.ts";
import { freshRedis, hasServices } from "./support.ts";

const past = new Date(Date.now() - 1_000);

describe("queueKey", () => {
    it("scopes a queue by concurrency key", () => {
        expect(queueKey("sync", null)).toBe("sync");
        expect(queueKey("sync", "customer-7")).toBe("sync:customer-7");
    });
});

describe.skipIf(!hasServices)("RedisQueue", () => {
    let redis: Redis;
    let queue: RedisQueue;

    beforeEach(async () => {
        redis ??= await freshRedis();
        await redis.flushdb();
        queue = new RedisQueue(redis);
    });

    afterAll(async () => {
        redis?.disconnect();
    });

    it("hands out due runs in order and skips future ones", async () => {
        await queue.enqueue("q", "run_b", new Date(past.getTime() + 10));
        await queue.enqueue("q", "run_a", past);
        await queue.enqueue("q", "run_later", new Date(Date.now() + 60_000));
        expect(await queue.dequeue("q", 10, 1_000)).toBe("run_a");
        expect(await queue.dequeue("q", 10, 1_000)).toBe("run_b");
        expect(await queue.dequeue("q", 10, 1_000)).toBeUndefined();
    });

    it("never exceeds the concurrency limit, even with many concurrent callers", async () => {
        await Promise.all(Array.from({ length: 20 }, (_, i) => queue.enqueue("q", `run_${i}`, past)));
        const taken = await Promise.all(Array.from({ length: 20 }, () => queue.dequeue("q", 3, 10_000)));
        expect(taken.filter(Boolean)).toHaveLength(3);
        expect(await queue.depth("q")).toEqual({ ready: 17, running: 3 });
    });

    it("frees a slot on release, and when a lease runs out", async () => {
        await Promise.all(["a", "b", "c"].map((id) => queue.enqueue("q", id, past)));
        const first = await queue.dequeue("q", 1, 10_000);
        expect(await queue.dequeue("q", 1, 10_000)).toBeUndefined();
        await queue.release("q", first ?? "");
        const second = await queue.dequeue("q", 1, 50);
        expect(second).toBeDefined();
        expect(await queue.dequeue("q", 1, 10_000, Date.now() + 100)).toBeDefined();
    });

    it("tracks which queues have work and forgets drained ones", async () => {
        await queue.enqueue("emails", "run_1", past);
        await queue.enqueue("sync:customer-7", "run_2", past);
        expect((await queue.activeQueues()).sort()).toEqual(["emails", "sync:customer-7"]);
        const id = await queue.dequeue("emails", 5, 1_000);
        await queue.release("emails", id ?? "");
        await queue.dequeue("emails", 5, 1_000);
        expect(await queue.activeQueues()).toEqual(["sync:customer-7"]);
    });

    it("extends leases only for runs that are still running", async () => {
        await queue.enqueue("q", "run_1", past);
        await queue.dequeue("q", 1, 50);
        await queue.extend("q", "run_1", 10_000);
        expect(await queue.dequeue("q", 1, 1_000, Date.now() + 100)).toBeUndefined();
        await queue.release("q", "run_1");
        await queue.extend("q", "run_1", 10_000);
        expect(await queue.depth("q")).toEqual({ ready: 0, running: 0 });
    });
});
