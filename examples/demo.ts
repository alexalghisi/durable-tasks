/**
 * Runs two workers against Postgres and Redis, triggers a mix of tasks and prints
 * every state change as it happens. One worker is killed mid-run to show recovery.
 *
 *   DATABASE_URL=postgres://… REDIS_URL=redis://… npm run demo
 */
import { Redis } from "ioredis";
import pg from "pg";
import { z } from "zod";
import { RedisQueue, RunStore, TaskClient, TaskRegistry, Worker, task } from "../src/index.ts";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const sendEmail = task({
    id: "send-email",
    schema: z.object({ to: z.email(), template: z.string() }),
    retry: { maxAttempts: 4, minTimeoutMs: 200, factor: 2 },
    run: async ({ to }, { attempt }) => {
        await sleep(80);
        // The provider rejects the first two attempts for one recipient.
        if (to.startsWith("grace") && attempt < 3) throw new Error("SMTP 421: try again later");
        return { messageId: `msg_${to.split("@")[0]}` };
    },
});

const syncCrm = task({
    id: "sync-crm",
    schema: z.object({ customer: z.string() }),
    queue: { concurrencyLimit: 1 }, // one sync per customer at a time, via concurrencyKey
    run: async ({ customer }) => {
        await sleep(250);
        return { customer, contacts: customer.length * 120 };
    },
});

const generateReport = task({
    id: "generate-report",
    maxDurationMs: 10_000,
    run: async (_: { month: string }, { attempt }) => {
        await sleep(attempt === 1 ? 60_000 : 300); // the first attempt hangs until its worker is killed
        return { pages: 12 };
    },
});

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL ?? "postgres://localhost/durable_tasks" });
const redis = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379");
await RunStore.migrate(pool);
await pool.query("TRUNCATE runs, run_events");
await redis.flushdb();

const store = new RunStore(pool);
const queue = new RedisQueue(redis);
const client = new TaskClient(store, queue);
const tasks = new TaskRegistry([sendEmail, syncCrm, generateReport]);
const quiet = { onError: () => {}, pollIntervalMs: 20, recoveryIntervalMs: 200 };
const alpha = new Worker({ id: "worker-alpha", tasks, store, queue, leaseMs: 1_000, ...quiet });
const beta = new Worker({ id: "worker-beta", tasks, store, queue, leaseMs: 1_000, ...quiet });

const started = Date.now();
const names = new Map<string, string>();
const seen = new Set<number>();
const colour = { triggered: 90, started: 36, retry_scheduled: 33, completed: 32, failed: 31, lease_expired: 35, cancelled: 90 } as const;
async function printEvents() {
    const { rows } = await pool.query<{ id: number; run_id: string; type: keyof typeof colour; attempt: number; detail: string | null; at: Date }>(
        "SELECT id, run_id, type, attempt, detail, at FROM run_events ORDER BY id",
    );
    for (const e of rows) {
        if (seen.has(e.id)) continue;
        seen.add(e.id);
        const t = ((e.at.getTime() - started) / 1000).toFixed(2).padStart(6);
        const detail = e.detail ? `  ${e.detail}` : "";
        console.log(`${t}s  ${(names.get(e.run_id) ?? e.run_id).padEnd(28)} \x1b[${colour[e.type]}m${e.type.padEnd(16)}\x1b[0m #${e.attempt}${detail}`);
    }
}

const trigger = async <T extends Parameters<typeof client.trigger>[0]>(label: string, t: T, payload: Parameters<typeof client.trigger<T>>[1], options = {}) => {
    const { id, created } = await client.trigger(t, payload, options);
    if (!names.has(id)) names.set(id, label);
    return { id, created };
};

alpha.start();
await trigger("report 2026-09", generateReport, { month: "2026-09" });
await sleep(150);
beta.start();
await trigger("email ada", sendEmail, { to: "ada@example.com", template: "welcome" }, { idempotencyKey: "welcome-ada" });
const again = await trigger("email ada (again)", sendEmail, { to: "ada@example.com", template: "welcome" }, { idempotencyKey: "welcome-ada" });
await trigger("email grace", sendEmail, { to: "grace@example.com", template: "welcome" });
for (const [label, customer] of [["sync acme #1", "acme"], ["sync acme #2", "acme"], ["sync globex", "globex"]] as const) {
    await trigger(label, syncCrm, { customer }, { concurrencyKey: customer });
}

const deadline = Date.now() + 8_000;
let killed = false;
while (Date.now() < deadline) {
    await printEvents();
    if (!killed && Date.now() - started > 600) {
        alpha.kill();
        killed = true;
        console.log(`  \x1b[1;31m--- worker-alpha killed (report attempt #1 still running) ---\x1b[0m`);
    }
    const open = await pool.query("SELECT count(*)::int AS n FROM runs WHERE status IN ('queued', 'executing')");
    if (killed && open.rows[0].n === 0) break;
    await sleep(50);
}
await printEvents();

console.log(`\nSecond trigger with idempotency key welcome-ada returned the same run: ${again.created ? "no" : "yes"}`);
console.log("\nrun                          status     attempts  output");
for (const run of (await store.list()).reverse()) {
    console.log(`${(names.get(run.id) ?? run.id).padEnd(28)} ${run.status.padEnd(10)} ${String(run.attempt).padEnd(9)} ${JSON.stringify(run.output)}`);
}
await beta.stop();
await pool.end();
redis.disconnect();
// worker-alpha's first report attempt is still sleeping in this process; a real crashed worker would be gone.
process.exit(0);
