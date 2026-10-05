export interface RetryOptions {
    /** Total attempts, including the first one. */
    readonly maxAttempts: number;
    readonly minTimeoutMs: number;
    readonly maxTimeoutMs: number;
    readonly factor: number;
    /** Spread retries by up to this fraction of the delay, so failed runs don't retry in lockstep. */
    readonly jitter: number;
}

export const DEFAULT_RETRY: RetryOptions = {
    maxAttempts: 3,
    minTimeoutMs: 1_000,
    maxTimeoutMs: 60_000,
    factor: 2,
    jitter: 0.2,
};

/** Delay before the attempt after `failedAttempt` (1-based): exponential, capped, with jitter. */
export function retryDelay(failedAttempt: number, options: RetryOptions, random: () => number = Math.random): number {
    const base = Math.min(options.maxTimeoutMs, options.minTimeoutMs * options.factor ** Math.max(0, failedAttempt - 1));
    const spread = base * options.jitter * (random() * 2 - 1);
    return Math.max(0, Math.min(options.maxTimeoutMs, Math.round(base + spread)));
}

export function shouldRetry(failedAttempt: number, options: RetryOptions): boolean {
    return failedAttempt < options.maxAttempts;
}
