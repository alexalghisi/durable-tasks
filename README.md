# durable-tasks

Background tasks for Node.js that survive what production throws at them: workers that crash mid-run, providers that return 503, a customer who triggers the same job twice, a Redis that restarts. You write an async function; the runtime handles retries, concurrency limits, recovery and tracing, on Postgres and Redis.

![Demo: retries, per-customer concurrency, idempotency and a worker killed mid-run, then the worker test suite](docs/demo.gif)

## Defining and triggering a task

```ts
import { z } from "zod";
import { TaskClient, task } from "durable-tasks";

export const syncCrm = task({
    id: "sync-crm",
    schema: z.object({ customer: z.string() }),           // validated when triggered and when run
    retry: { maxAttempts: 5, minTimeoutMs: 1_000, factor: 2 },
    queue: { concurrencyLimit: 1 },                       // per concurrency key, across all workers
    maxDurationMs: 120_000,
    run: async ({ customer }, { attempt, signal }) => {
        return crm.sync(customer, { signal });
    },
});

await client.trigger(syncCrm, { customer: "acme" }, {
    concurrencyKey: "acme",                               // one sync per customer at a time
    idempotencyKey: "acme-2026-10-05",                    // triggering twice returns the first run
});
```

`trigger` is type-checked against the task's payload, and invalid payloads are rejected before anything is stored.

## How it works

Postgres is the source of truth. Redis only does what it is good at: atomic queue operations.

1. **Trigger.** The run is inserted into Postgres, with `ON CONFLICT` handling the idempotency key, and then added to a Redis sorted set scored by due time.
2. **Dequeue.** A Lua script checks the queue's concurrency limit and takes the next due run in one atomic step. Two workers can never both take the last free slot. With a concurrency key, the limit applies per key, so a busy customer can't starve the others.
3. **Claim.** A conditional `UPDATE … WHERE status = 'queued'` starts the attempt and gives the worker a lease. Only one worker can win it.
4. **Execute.** Heartbeats renew the lease in Postgres and Redis. The task gets an `AbortSignal` that fires on `maxDurationMs`, on cancellation, or on shutdown.
5. **Finish.** Completing or failing a run is fenced by `(worker, attempt)`. A worker that lost its lease, because it was paused, partitioned or presumed dead, can't overwrite the result of a newer attempt. Failures are retried with exponential backoff and jitter until `maxAttempts`.

**When things go wrong:**

| Failure | What happens |
| --- | --- |
| Worker crashes mid-attempt | Its lease expires. Recovery requeues the run, and the next attempt runs on a healthy worker. If that was the last allowed attempt, the run fails with a clear reason instead of looping. |
| Task hangs | `maxDurationMs` aborts it, and it is retried like any other failure. |
| Deploy or shutdown | `worker.stop(graceMs)` stops taking work and waits. Attempts still running after the grace period are aborted and retried elsewhere. |
| Redis loses data | Runs are still queued in Postgres. Recovery restores them to Redis without disturbing runs in flight. |
| Redis is down at trigger time | The run is already in Postgres, so recovery queues it once Redis is back. |
| Duplicate trigger | The same idempotency key returns the first run, even with concurrent triggers. |

Every transition is also written to `run_events` in the same statement as the change. Each run has a complete timeline: triggered, started, retry scheduled, lease expired, completed.

**Tracing.** Triggers and attempts produce OpenTelemetry spans through `@opentelemetry/api`. They go to whatever SDK your application registers, and cost nothing without one. Attempts often run in another process minutes later, so they *link* to the trigger span instead of nesting under it. They carry `run.id`, `run.attempt`, `worker.id` and `run.outcome`.

## Tech stack

Node.js 22, TypeScript (strict), PostgreSQL (`pg`), Redis (`ioredis`, Lua), zod, OpenTelemetry, vitest

## Running it

```bash
docker compose up -d                 # Postgres 16 and Redis 7
npm ci
DATABASE_URL=postgres://tasks:tasks@localhost/durable_tasks REDIS_URL=redis://localhost:6379 npm run demo
```

The demo starts two workers and triggers emails (one recipient's provider fails twice), per-customer CRM syncs and a report. It kills one worker while the report is running and prints every state change as it happens.

A worker in your own app:

```ts
const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
await RunStore.migrate(pool);
const store = new RunStore(pool);
const queue = new RedisQueue(new Redis(process.env.REDIS_URL));

const worker = new Worker({ tasks: new TaskRegistry([syncCrm, sendEmail]), store, queue, concurrency: 20 });
worker.start();
process.on("SIGTERM", () => worker.stop(30_000));
```

## Tests

```bash
npm run typecheck
DURABLE_TASKS_TEST_DATABASE_URL=postgres://tasks:tasks@localhost/durable_tasks \
DURABLE_TASKS_TEST_REDIS_URL=redis://localhost:6379/1 npm test
```

The integration tests use real Postgres and Redis, and are skipped when those variables aren't set. They cover:

- concurrent claims and idempotent triggers;
- concurrency limits across three workers;
- per-key limits;
- a worker killed mid-attempt, including its late result being rejected;
- attempt timeouts, cancellation and graceful shutdown;
- rebuilding Redis from Postgres;
- trace links.

## License

MIT
