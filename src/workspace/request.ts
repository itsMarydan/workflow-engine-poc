export const workflowEngines = ["inngest", "temporal"] as const;

export type WorkflowEngine = (typeof workflowEngines)[number];

export type ProvisionWorkspaceRequest = {
  requestId: string;
  workspaceId: string;
  engines: WorkflowEngine[];
};

export function parseProvisionWorkspaceRequest(value: unknown): ProvisionWorkspaceRequest {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError("Request body must be an object");
  }

  const request = value as Record<string, unknown>;
  if (typeof request.requestId !== "string" || request.requestId.trim().length === 0) {
    throw new TypeError("requestId must be a non-empty string");
  }
  if (typeof request.workspaceId !== "string" || request.workspaceId.trim().length === 0) {
    throw new TypeError("workspaceId must be a non-empty string");
  }
  if (!Array.isArray(request.engines) || request.engines.length === 0) {
    throw new TypeError("engines must be a non-empty array");
  }

  const engines: WorkflowEngine[] = [];
  for (const engine of request.engines) {
    if (typeof engine !== "string" || !workflowEngines.includes(engine as WorkflowEngine)) {
      throw new TypeError(`Unsupported workflow engine: ${String(engine)}`);
    }
    if (engines.includes(engine as WorkflowEngine)) {
      throw new TypeError(`Duplicate workflow engine: ${engine}`);
    }
    engines.push(engine as WorkflowEngine);
  }

  return {
    requestId: request.requestId,
    workspaceId: request.workspaceId,
    engines,
  };
}