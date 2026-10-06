import { proxyActivities, sleep } from "@temporalio/workflow";

import type { ProvisionWorkspaceRequest } from "./request.js";
import { callMockService, createProvisionRequestPayload } from "./service-client.js";

export const TEMPORAL_TASK_QUEUE = "workspace-provisioning" as const;

export const activities = {
  provisionData: async (request: ProvisionWorkspaceRequest) => {
    return await callMockService({
      service: "data",
      workspaceId: request.workspaceId,
      requestId: request.requestId,
      payload: createProvisionRequestPayload(request, "data"),
    });
  },
  provisionStorage: async (request: ProvisionWorkspaceRequest) => {
    return await callMockService({
      service: "storage",
      workspaceId: request.workspaceId,
      requestId: request.requestId,
      payload: createProvisionRequestPayload(request, "storage"),
    });
  },
  provisionBilling: async (request: ProvisionWorkspaceRequest) => {
    return await callMockService({
      service: "billing",
      workspaceId: request.workspaceId,
      requestId: request.requestId,
      payload: createProvisionRequestPayload(request, "billing"),
    });
  },
};

const { provisionData, provisionStorage, provisionBilling } = proxyActivities<typeof activities>({
  startToCloseTimeout: "5 minutes",
  retry: {
    initialInterval: "1s",
    maximumInterval: "30s",
    maximumAttempts: 5,
  },
});

export async function provisionWorkspaceWorkflow(request: ProvisionWorkspaceRequest): Promise<{
  engine: "temporal";
  requestId: string;
  workspaceId: string;
  status: "provisioned";
  steps: {
    data: Awaited<ReturnType<typeof callMockService>>;
    storage: Awaited<ReturnType<typeof callMockService>>;
    billing: Awaited<ReturnType<typeof callMockService>>;
  };
}> {
  const data = await provisionData(request);
  const storage = await provisionStorage(request);
  await sleep(1500);
  const billing = await provisionBilling(request);

  return {
    engine: "temporal",
    requestId: request.requestId,
    workspaceId: request.workspaceId,
    status: "provisioned",
    steps: {
      data,
      storage,
      billing,
    },
  };
}
