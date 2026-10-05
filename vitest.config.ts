import { defineConfig } from "vitest/config";

export default defineConfig({
    test: {
        include: ["test/**/*.test.ts"],
        // Integration tests share one Postgres database and one Redis instance.
        fileParallelism: false,
        testTimeout: 20_000,
    },
});
