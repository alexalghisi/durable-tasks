import { SpanStatusCode, trace } from "@opentelemetry/api";
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base";
import type { Redis } from "ioredis";
import type pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { TaskClient } from "../src/client.ts";
import { RedisQueue } from "../src/queue/RedisQueue.ts";
import { RunStore } from "../src/store/RunStore.ts";
import { TaskRegistry, task } from "../src/task.ts";
import { Worker } from "../src/worker.ts";
import { eventually, freshDatabase, freshRedis, hasServices } from "./support.ts";

describe.skipIf(!hasServices)("tracing", () => {
    const exporter = new InMemorySpanExporter();
    let pool: pg.Pool;
    let redis: Redis;

    beforeAll(async () => {
        trace.setGlobalTracerProvider(new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] }));
        pool = await freshDatabase();
        redis = await freshRedis();
    });

    afterAll(async () => {
        trace.disable();
        await pool.end();
        redis.disconnect();
    });

    it("links each attempt span to the trigger span, with outcome and errors", async () => {
        let attempts = 0;
        const flaky = task({
            id: "traced",
            retry: { maxAttempts: 2, minTimeoutMs: 10, jitter: 0 },
            run: async () => {
                if (++attempts === 1) throw new Error("upstream 503");
                return "ok";
            },
        });
        const store = new RunStore(pool);
        const queue = new RedisQueue(redis);
        const worker = new Worker({ id: "w-traced", tasks: new TaskRegistry([flaky]), store, queue, pollIntervalMs: 10 });
        worker.start();
        const { id } = await new TaskClient(store, queue).trigger(flaky, {});
        await eventually(async () => (await store.get(id))?.status === "completed");
        await worker.stop();

        const spans = exporter.getFinishedSpans();
        const trigger = spans.find((s) => s.name === "trigger traced");
        const runs = spans.filter((s) => s.name === "run traced");
        expect(trigger?.attributes).toMatchObject({ "run.id": id, "run.created": true });
        expect(runs.map((s) => s.attributes["run.outcome"])).toEqual(["retry_scheduled", "completed"]);
        expect(runs[0]?.status).toMatchObject({ code: SpanStatusCode.ERROR, message: "upstream 503" });
        for (const span of runs) {
            expect(span.links[0]?.context.spanId).toBe(trigger?.spanContext().spanId);
            expect(span.attributes).toMatchObject({ "run.id": id, "worker.id": "w-traced" });
        }
        expect(await store.get(id)).toMatchObject({ traceId: trigger?.spanContext().traceId, spanId: trigger?.spanContext().spanId });
    });
});
