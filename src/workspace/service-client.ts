import type { ProvisionWorkspaceRequest } from "./request.js";

export type MockServiceName = "data" | "storage" | "billing";

export type MockServiceStepResult = {
  service: MockServiceName;
  workspaceId: string;
  requestId: string;
  resourceId: string;
  idempotencyKey: string;
  attempt: number;
  replayed: boolean;
  status: "created" | "replayed";
};

export async function callMockService(args: {
  service: MockServiceName;
  workspaceId: string;
  requestId: string;
  payload: Record<string, unknown>;
}): Promise<MockServiceStepResult> {
  const { service, workspaceId, requestId, payload } = args;
  const serviceBaseUrls = {
    data: process.env.DATA_SERVICE_URL ?? "http://127.0.0.1:4101",
    storage: process.env.STORAGE_SERVICE_URL ?? "http://127.0.0.1:4102",
    billing: process.env.BILLING_SERVICE_URL ?? "http://127.0.0.1:4103",
  } as const;
  const idempotencyKey = `${requestId}:${service}`;
  const route = service === "billing" ? "billing-profiles" : "resources";
  const baseUrl = serviceBaseUrls[service].replace(/\/$/, "");
  const url = `${baseUrl}/workspaces/${encodeURIComponent(workspaceId)}/${route}`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "idempotency-key": idempotencyKey,
    },
    body: JSON.stringify({
      ...payload,
      requestId,
      workspaceId,
      service,
      createdBy: "workflow-engine-poc",
    }),
  });

  const body = (await response.json()) as Record<string, unknown>;
  if (!response.ok) {
    const error = body.error ?? "Downstream service failed";
    throw new Error(`${service} mock service rejected request: ${String(error)}`);
  }

  return {
    service,
    workspaceId,
    requestId,
    resourceId: String(body.resourceId ?? `${service}-${workspaceId}`),
    idempotencyKey,
    attempt: Number(body.attempt ?? 0),
    replayed: Boolean(body.replayed),
    status: body.replayed ? "replayed" : "created",
  };
}

export function createProvisionRequestPayload(
  request: ProvisionWorkspaceRequest,
  step: "data" | "storage" | "billing",
): Record<string, unknown> {
  switch (step) {
    case "data":
      return {
        kind: "data-resources",
        engines: request.engines,
      };
    case "storage":
      return {
        kind: "storage-resources",
        source: "data-provisioning",
        engines: request.engines,
      };
    case "billing":
      return {
        kind: "billing-profile",
        plan: "starter",
        engines: request.engines,
      };
    default:
      return { kind: step, engines: request.engines };
  }
}
