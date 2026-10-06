import { NativeConnection, Worker } from "@temporalio/worker";

import { TEMPORAL_TASK_QUEUE, activities } from "./workspace/temporal.js";

async function main(): Promise<void> {
  const connection = await NativeConnection.connect({ address: "localhost:7233" });

  const worker = await Worker.create({
    connection,
    namespace: "default",
    taskQueue: TEMPORAL_TASK_QUEUE,
    workflowsPath: new URL("./workspace/temporal.ts", import.meta.url).pathname,
    activities,
  });

  console.log(`Temporal worker listening on task queue ${TEMPORAL_TASK_QUEUE}`);
  await worker.run();
}

void main().catch((error: unknown) => {
  console.error("Temporal worker failed", error);
  process.exit(1);
});
