export type RunStatus = "queued" | "executing" | "completed" | "failed" | "cancelled";

export const TERMINAL_STATUSES: readonly RunStatus[] = ["completed", "failed", "cancelled"];

export interface Run {
    readonly id: string;
    readonly taskId: string;
    readonly status: RunStatus;
    readonly payload: unknown;
    readonly output: unknown;
    readonly error: string | null;
    /** Attempts started so far, including the one that is executing. */
    readonly attempt: number;
    readonly queue: string;
    readonly concurrencyKey: string | null;
    readonly idempotencyKey: string | null;
    readonly runAt: Date;
    readonly lockedBy: string | null;
    readonly leaseUntil: Date | null;
    readonly traceId: string | null;
    readonly spanId: string | null;
    readonly createdAt: Date;
    readonly updatedAt: Date;
}

export type RunEventType = "triggered" | "started" | "retry_scheduled" | "completed" | "failed" | "lease_expired" | "cancelled";

export interface RunEvent {
    readonly runId: string;
    readonly type: RunEventType;
    readonly attempt: number;
    readonly detail: string | null;
    readonly at: Date;
}
