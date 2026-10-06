import { Inngest } from "inngest";

import { parseProvisionWorkspaceRequest, type ProvisionWorkspaceRequest } from "./request.js";
import { callMockService, createProvisionRequestPayload } from "./service-client.js";

export const WORKSPACE_PROVISION_REQUESTED = "workspace/provision.requested" as const;

export const inngest = new Inngest({
  id: "workflow-engine-poc",
});

export const provisionWorkspaceWorkflow = inngest.createFunction(
  {
    id: "provision-workspace",
    name: "Provision workspace",
    triggers: [{ event: WORKSPACE_PROVISION_REQUESTED }],
  },
  async ({ event, step }: { event: { data: unknown }; step: {
    run: <T>(name: string, fn: () => Promise<T>) => Promise<T>;
    sleep: (name: string, duration: string | number) => Promise<void>;
  } }) => {
    const request = parseProvisionWorkspaceRequest(event.data);

    const dataResult = await step.run("provision-data-resources", async () => {
      return await callMockService({
        service: "data",
        workspaceId: request.workspaceId,
        requestId: request.requestId,
        payload: createProvisionRequestPayload(request, "data"),
      });
    });

    const storageResult = await step.run("create-storage-resources", async () => {
      return await callMockService({
        service: "storage",
        workspaceId: request.workspaceId,
        requestId: request.requestId,
        payload: createProvisionRequestPayload(request, "storage"),
      });
    });

    const billingDelayMs = Number(process.env.WORKSPACE_BILLING_DELAY_MS ?? "1500");
    const billingDelay = Number.isFinite(billingDelayMs) && billingDelayMs > 0 ? `${billingDelayMs}ms` : "1s";
    await step.sleep("billing-delay", billingDelay);

    const billingResult = await step.run("create-billing-profile", async () => {
      return await callMockService({
        service: "billing",
        workspaceId: request.workspaceId,
        requestId: request.requestId,
        payload: createProvisionRequestPayload(request, "billing"),
      });
    });

    return {
      engine: "inngest",
      requestId: request.requestId,
      workspaceId: request.workspaceId,
      status: "provisioned",
      steps: {
        data: dataResult,
        storage: storageResult,
        billing: billingResult,
      },
    };
  },
);

export async function triggerProvisionWorkspace(request: ProvisionWorkspaceRequest): Promise<void> {
  await inngest.send({
    name: WORKSPACE_PROVISION_REQUESTED,
    data: request,
  });
}
