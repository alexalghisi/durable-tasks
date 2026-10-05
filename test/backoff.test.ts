import { describe, expect, it } from "vitest";
import { DEFAULT_RETRY, retryDelay, shouldRetry } from "../src/backoff.ts";

const noJitter = { ...DEFAULT_RETRY, jitter: 0 };

describe("retryDelay", () => {
    it("grows exponentially from the minimum", () => {
        expect([1, 2, 3, 4].map((a) => retryDelay(a, noJitter))).toEqual([1_000, 2_000, 4_000, 8_000]);
    });

    it("is capped at the maximum, jitter included", () => {
        expect(retryDelay(20, noJitter)).toBe(60_000);
        expect(retryDelay(20, DEFAULT_RETRY, () => 1)).toBe(60_000);
    });

    it("spreads delays within the jitter band", () => {
        expect(retryDelay(2, DEFAULT_RETRY, () => 0)).toBe(1_600);
        expect(retryDelay(2, DEFAULT_RETRY, () => 1)).toBe(2_400);
    });
});

describe("shouldRetry", () => {
    it("counts the first attempt", () => {
        expect(shouldRetry(1, { ...DEFAULT_RETRY, maxAttempts: 2 })).toBe(true);
        expect(shouldRetry(2, { ...DEFAULT_RETRY, maxAttempts: 2 })).toBe(false);
    });
});
