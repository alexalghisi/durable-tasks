import { describe, expectTypeOf, it } from "vitest";
import { z } from "zod";
import type { TaskClient } from "../src/client.ts";
import { task, type OutputOf, type PayloadOf } from "../src/task.ts";

const syncCrm = task({ id: "sync-crm", schema: z.object({ customer: z.string() }), run: async ({ customer }) => customer.length });

describe("types", () => {
    it("infers payload and output types from the task", () => {
        expectTypeOf<PayloadOf<typeof syncCrm>>().toEqualTypeOf<{ customer: string }>();
        expectTypeOf<OutputOf<typeof syncCrm>>().toEqualTypeOf<number>();
    });

    it("type-checks trigger payloads", () => {
        const trigger = (client: TaskClient) => {
            void client.trigger(syncCrm, { customer: "acme" });
            // @ts-expect-error customer must be a string
            void client.trigger(syncCrm, { customer: 42 });
        };
        expectTypeOf(trigger).toBeFunction();
    });
});
