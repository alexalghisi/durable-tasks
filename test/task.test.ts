import { describe, expect, it } from "vitest";
import { z } from "zod";
import { DEFAULT_RETRY } from "../src/backoff.ts";
import { DEFAULT_CONCURRENCY, TaskRegistry, task } from "../src/task.ts";

const noop = async () => undefined;

describe("task", () => {
    it("fills in defaults", () => {
        const t = task({ id: "send-email", run: noop });
        expect(t.retry).toEqual(DEFAULT_RETRY);
        expect(t.queue).toEqual({ name: "send-email", concurrencyLimit: DEFAULT_CONCURRENCY });
    });

    it("merges partial retry options and named queues", () => {
        const t = task({ id: "sync", retry: { maxAttempts: 5 }, queue: { name: "crm", concurrencyLimit: 2 }, run: noop });
        expect(t.retry.maxAttempts).toBe(5);
        expect(t.retry.factor).toBe(DEFAULT_RETRY.factor);
        expect(t.queue).toEqual({ name: "crm", concurrencyLimit: 2 });
    });

    it("validates payloads against the schema before running", async () => {
        const t = task({ id: "typed", schema: z.object({ to: z.string() }), run: async ({ to }) => to.length });
        const context = { runId: "run_1", attempt: 1, signal: new AbortController().signal };
        await expect(t.execute({ to: "ada" }, context)).resolves.toBe(3);
        expect(() => t.parsePayload({ to: 1 })).toThrow('Invalid payload for task "typed": to: Invalid input: expected string, received number');
    });

    it("can be registered next to tasks with other payload types", () => {
        const typed = task({ id: "typed", schema: z.object({ to: z.string() }), run: async ({ to }) => to });
        expect(new TaskRegistry([typed, task({ id: "untyped", run: noop })]).all).toHaveLength(2);
    });

    it("rejects invalid configuration early", () => {
        expect(() => task({ id: "Bad Id", run: noop })).toThrow(/Invalid task id/);
        expect(() => task({ id: "t", retry: { maxAttempts: 0 }, run: noop })).toThrow(/maxAttempts/);
        expect(() => task({ id: "t", queue: { concurrencyLimit: 0 }, run: noop })).toThrow(/concurrencyLimit/);
    });
});

describe("TaskRegistry", () => {
    it("refuses duplicate ids", () => {
        const registry = new TaskRegistry([task({ id: "a", run: noop })]);
        expect(() => registry.register(task({ id: "a", run: noop }))).toThrow(/registered twice/);
        expect(registry.get("a")?.id).toBe("a");
    });
});
