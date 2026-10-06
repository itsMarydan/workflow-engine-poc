import { Client, Connection } from "@temporalio/client";

import { parseProvisionWorkspaceRequest, type ProvisionWorkspaceRequest } from "./request.js";
import { TEMPORAL_TASK_QUEUE } from "./temporal.js";

export async function triggerTemporalProvision(request: unknown): Promise<string> {
  const parsed = parseProvisionWorkspaceRequest(request);
  const connection = await Connection.connect({ address: "localhost:7233" });
  const client = new Client({ connection });

  const handle = await client.workflow.start("provisionWorkspaceWorkflow", {
    args: [parsed],
    taskQueue: TEMPORAL_TASK_QUEUE,
    workflowId: `workspace-provision-${parsed.requestId}`,
  });

  return handle.workflowId;
}
