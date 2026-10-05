import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import type pg from "pg";
import type { Run, RunEvent, RunEventType, RunStatus } from "../types.ts";

export interface NewRun {
    readonly taskId: string;
    readonly payload: unknown;
    readonly queue: string;
    readonly concurrencyKey: string | null;
    readonly idempotencyKey: string | null;
    readonly runAt: Date;
    readonly traceId: string | null;
    readonly spanId: string | null;
}

/** The worker that owns an attempt. Writes are only accepted from the current owner. */
export interface Lease {
    readonly runId: string;
    readonly workerId: string;
    readonly attempt: number;
}

interface RunRow {
    id: string;
    task_id: string;
    status: RunStatus;
    payload: unknown;
    output: unknown;
    error: string | null;
    attempt: number;
    queue: string;
    concurrency_key: string | null;
    idempotency_key: string | null;
    run_at: Date;
    locked_by: string | null;
    lease_until: Date | null;
    trace_id: string | null;
    span_id: string | null;
    created_at: Date;
    updated_at: Date;
}

function toRun(row: RunRow): Run {
    return {
        id: row.id,
        taskId: row.task_id,
        status: row.status,
        payload: row.payload,
        output: row.output,
        error: row.error,
        attempt: row.attempt,
        queue: row.queue,
        concurrencyKey: row.concurrency_key,
        idempotencyKey: row.idempotency_key,
        runAt: row.run_at,
        lockedBy: row.locked_by,
        leaseUntil: row.lease_until,
        traceId: row.trace_id,
        spanId: row.span_id,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
    };
}

/** Wraps a run-changing statement so the event is written in the same statement. */
function withEvent(update: string, type: RunEventType, detail = "NULL"): string {
    return `WITH r AS (${update} RETURNING *),
            e AS (INSERT INTO run_events (run_id, type, attempt, detail) SELECT id, '${type}', attempt, ${detail} FROM r)
            SELECT * FROM r`;
}

const OWNED = "id = $1 AND locked_by = $2 AND attempt = $3 AND status = 'executing'";

/**
 * Postgres is the source of truth for runs. Every transition is a single
 * conditional UPDATE, so two workers can never both own an attempt, and a worker
 * that lost its lease can't complete or fail a run that someone else now owns.
 */
export class RunStore {
    readonly #pool: pg.Pool;

    constructor(pool: pg.Pool) {
        this.#pool = pool;
    }

    static async migrate(pool: pg.Pool): Promise<void> {
        await pool.query(await readFile(new URL("./schema.sql", import.meta.url), "utf8"));
    }

    async create(run: NewRun): Promise<{ run: Run; created: boolean }> {
        const id = `run_${randomUUID().replaceAll("-", "").slice(0, 20)}`;
        const inserted = await this.#pool.query<RunRow>(
            withEvent(
                `INSERT INTO runs (id, task_id, status, payload, queue, concurrency_key, idempotency_key, run_at, trace_id, span_id)
                 VALUES ($1, $2, 'queued', $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (task_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`,
                "triggered",
            ),
            [id, run.taskId, JSON.stringify(run.payload ?? null), run.queue, run.concurrencyKey, run.idempotencyKey, run.runAt, run.traceId, run.spanId],
        );
        const row = inserted.rows[0];
        if (row) return { run: toRun(row), created: true };
        const existing = await this.#pool.query<RunRow>("SELECT * FROM runs WHERE task_id = $1 AND idempotency_key = $2", [run.taskId, run.idempotencyKey]);
        const found = existing.rows[0];
        if (!found) throw new Error(`Idempotency conflict for ${run.taskId}/${run.idempotencyKey} but no run found`);
        return { run: toRun(found), created: false };
    }

    async get(id: string): Promise<Run | undefined> {
        const { rows } = await this.#pool.query<RunRow>("SELECT * FROM runs WHERE id = $1", [id]);
        return rows[0] && toRun(rows[0]);
    }

    async list(options: { status?: RunStatus; limit?: number } = {}): Promise<Run[]> {
        const { rows } = await this.#pool.query<RunRow>(
            "SELECT * FROM runs WHERE ($1::text IS NULL OR status = $1) ORDER BY created_at DESC, id LIMIT $2",
            [options.status ?? null, options.limit ?? 100],
        );
        return rows.map(toRun);
    }

    async events(runId: string): Promise<RunEvent[]> {
        const { rows } = await this.#pool.query<{ run_id: string; type: RunEventType; attempt: number; detail: string | null; at: Date }>(
            "SELECT run_id, type, attempt, detail, at FROM run_events WHERE run_id = $1 ORDER BY id",
            [runId],
        );
        return rows.map((r) => ({ runId: r.run_id, type: r.type, attempt: r.attempt, detail: r.detail, at: r.at }));
    }

    /** Starts the next attempt if the run is queued and due. Returns undefined if someone else got there first. */
    async claim(runId: string, workerId: string, leaseMs: number): Promise<Run | undefined> {
        const { rows } = await this.#pool.query<RunRow>(
            withEvent(
                `UPDATE runs SET status = 'executing', attempt = attempt + 1, locked_by = $2,
                        lease_until = now() + make_interval(secs => $3::double precision / 1000), updated_at = now()
                 WHERE id = $1 AND status = 'queued' AND run_at <= now()`,
                "started",
                "r.locked_by",
            ),
            [runId, workerId, leaseMs],
        );
        return rows[0] && toRun(rows[0]);
    }

    /** Extends the lease. False means the attempt is no longer ours: cancelled, or expired and reclaimed. */
    async heartbeat(lease: Lease, leaseMs: number): Promise<boolean> {
        const { rowCount } = await this.#pool.query(
            `UPDATE runs SET lease_until = now() + make_interval(secs => $4::double precision / 1000), updated_at = now() WHERE ${OWNED}`,
            [lease.runId, lease.workerId, lease.attempt, leaseMs],
        );
        return rowCount === 1;
    }

    async complete(lease: Lease, output: unknown): Promise<boolean> {
        const { rows } = await this.#pool.query<RunRow>(
            withEvent(
                `UPDATE runs SET status = 'completed', output = $4, error = NULL, locked_by = NULL, lease_until = NULL, updated_at = now() WHERE ${OWNED}`,
                "completed",
            ),
            [lease.runId, lease.workerId, lease.attempt, JSON.stringify(output ?? null)],
        );
        return rows.length === 1;
    }

    /** Records a failed attempt: queued again at `retryAt`, or failed for good when retryAt is null. */
    async fail(lease: Lease, error: string, retryAt: Date | null): Promise<boolean> {
        const sql =
            retryAt === null
                ? withEvent(`UPDATE runs SET status = 'failed', error = $4, locked_by = NULL, lease_until = NULL, updated_at = now() WHERE ${OWNED}`, "failed", "r.error")
                : withEvent(
                      `UPDATE runs SET status = 'queued', error = $4, run_at = $5, locked_by = NULL, lease_until = NULL, updated_at = now() WHERE ${OWNED}`,
                      "retry_scheduled",
                      "r.error",
                  );
        const params = [lease.runId, lease.workerId, lease.attempt, error, ...(retryAt === null ? [] : [retryAt])];
        const { rows } = await this.#pool.query<RunRow>(sql, params);
        return rows.length === 1;
    }

    /** Puts runs whose worker stopped renewing its lease back in the queue. */
    async requeueExpired(limit = 100): Promise<Run[]> {
        const { rows } = await this.#pool.query<RunRow>(
            withEvent(
                `UPDATE runs SET status = 'queued', run_at = now(), locked_by = NULL, lease_until = NULL, updated_at = now()
                 WHERE id IN (SELECT id FROM runs WHERE status = 'executing' AND lease_until < now()
                              ORDER BY lease_until LIMIT $1 FOR UPDATE SKIP LOCKED)`,
                "lease_expired",
            ),
            [limit],
        );
        return rows.map(toRun);
    }

    async cancel(runId: string): Promise<Run | undefined> {
        const { rows } = await this.#pool.query<RunRow>(
            withEvent(
                `UPDATE runs SET status = 'cancelled', locked_by = NULL, lease_until = NULL, updated_at = now()
                 WHERE id = $1 AND status IN ('queued', 'executing')`,
                "cancelled",
            ),
            [runId],
        );
        return rows[0] && toRun(rows[0]);
    }

    /** Queued runs, oldest first, for rebuilding the Redis queues. */
    async queued(limit: number, afterId = ""): Promise<Run[]> {
        const { rows } = await this.#pool.query<RunRow>("SELECT * FROM runs WHERE status = 'queued' AND id > $1 ORDER BY id LIMIT $2", [afterId, limit]);
        return rows.map(toRun);
    }
}
