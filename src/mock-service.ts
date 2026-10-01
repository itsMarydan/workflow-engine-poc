import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { Pool, type PoolClient } from "pg";

type Operation = {
  service: string;
  workspaceId: string;
  idempotencyKey: string;
  resourceId: string;
  createdAt: string;
};

const serviceName = process.env.MOCK_SERVICE_NAME;
const port = Number(process.env.PORT ?? 4100);
const initialFailFirst = Number(process.env.FAIL_FIRST_N ?? 0);
const databaseUrl = process.env.WORKFLOW_POC_DATABASE_URL;

if (
  !serviceName ||
  !Number.isInteger(initialFailFirst) ||
  initialFailFirst < 0 ||
  !databaseUrl
) {
  throw new Error("Set MOCK_SERVICE_NAME, WORKFLOW_POC_DATABASE_URL, and a non-negative FAIL_FIRST_N");
}

const pool = new Pool({ connectionString: databaseUrl, max: 5 });
let failFirst = initialFailFirst;

function sendJson(response: ServerResponse, status: number, value: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(value));
}

async function readJson(request: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  if (chunks.length === 0) {
    return {};
  }

  return JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
}

async function initializeDatabase(): Promise<void> {
  await pool.query(`
    CREATE SCHEMA IF NOT EXISTS workflow_poc;

    CREATE TABLE IF NOT EXISTS workflow_poc.mock_attempt_counters (
      service_name text NOT NULL,
      idempotency_key text NOT NULL,
      attempt_count integer NOT NULL,
      PRIMARY KEY (service_name, idempotency_key)
    );

    CREATE TABLE IF NOT EXISTS workflow_poc.mock_attempts (
      id bigserial PRIMARY KEY,
      service_name text NOT NULL,
      idempotency_key text NOT NULL,
      attempt_number integer NOT NULL,
      outcome text NOT NULL DEFAULT 'started',
      recorded_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE (service_name, idempotency_key, attempt_number)
    );

    CREATE TABLE IF NOT EXISTS workflow_poc.mock_operations (
      id bigserial PRIMARY KEY,
      service_name text NOT NULL,
      workspace_id text NOT NULL,
      idempotency_key text NOT NULL,
      resource_id text NOT NULL,
      request_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
      created_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE (service_name, idempotency_key)
    );
  `);
}

async function recordAttempt(idempotencyKey: string): Promise<{ id: string; number: number }> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const counter = await client.query<{ attempt_count: number }>(
      `INSERT INTO workflow_poc.mock_attempt_counters (service_name, idempotency_key, attempt_count)
       VALUES ($1, $2, 1)
       ON CONFLICT (service_name, idempotency_key)
       DO UPDATE SET attempt_count = workflow_poc.mock_attempt_counters.attempt_count + 1
       RETURNING attempt_count`,
      [serviceName, idempotencyKey],
    );
    const number = counter.rows[0].attempt_count;
    const attempt = await client.query<{ id: string }>(
      `INSERT INTO workflow_poc.mock_attempts (service_name, idempotency_key, attempt_number)
       VALUES ($1, $2, $3)
       RETURNING id`,
      [serviceName, idempotencyKey, number],
    );
    await client.query("COMMIT");
    return { id: attempt.rows[0].id, number };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

async function updateAttempt(id: string, outcome: string): Promise<void> {
  await pool.query(
    "UPDATE workflow_poc.mock_attempts SET outcome = $1 WHERE id = $2",
    [outcome, id],
  );
}

async function findOperation(client: PoolClient, idempotencyKey: string): Promise<Operation | undefined> {
  const result = await client.query<Operation>(
    `SELECT service_name AS service, workspace_id AS "workspaceId",
            idempotency_key AS "idempotencyKey", resource_id AS "resourceId",
            created_at AS "createdAt"
     FROM workflow_poc.mock_operations
     WHERE service_name = $1 AND idempotency_key = $2`,
    [serviceName, idempotencyKey],
  );
  return result.rows[0];
}

async function listState(): Promise<unknown> {
  const [attempts, operations] = await Promise.all([
    pool.query(
      `SELECT id, idempotency_key AS "idempotencyKey", attempt_number AS "attemptNumber",
              outcome, recorded_at AS "recordedAt"
       FROM workflow_poc.mock_attempts
       WHERE service_name = $1
       ORDER BY id`,
      [serviceName],
    ),
    pool.query(
      `SELECT workspace_id AS "workspaceId", idempotency_key AS "idempotencyKey",
              resource_id AS "resourceId", request_payload AS "requestPayload",
              created_at AS "createdAt"
       FROM workflow_poc.mock_operations
       WHERE service_name = $1
       ORDER BY id`,
      [serviceName],
    ),
  ]);
  return { service: serviceName, failFirst, attempts: attempts.rows, operations: operations.rows };
}

async function resetState(body: { failFirst?: unknown }): Promise<void> {
  if (
    body.failFirst !== undefined &&
    (!Number.isInteger(body.failFirst) || Number(body.failFirst) < 0)
  ) {
    throw new RangeError("failFirst must be a non-negative integer");
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("DELETE FROM workflow_poc.mock_operations WHERE service_name = $1", [serviceName]);
    await client.query("DELETE FROM workflow_poc.mock_attempts WHERE service_name = $1", [serviceName]);
    await client.query("DELETE FROM workflow_poc.mock_attempt_counters WHERE service_name = $1", [serviceName]);
    await client.query("COMMIT");
    failFirst = body.failFirst === undefined ? initialFailFirst : Number(body.failFirst);
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

async function handleRequest(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);

  if (request.method === "GET" && url.pathname === "/health") {
    await pool.query("SELECT 1");
    sendJson(response, 200, { status: "ok", service: serviceName, database: "ok" });
    return;
  }

  if (request.method === "GET" && url.pathname === "/_admin/state") {
    sendJson(response, 200, await listState());
    return;
  }

  if (request.method === "POST" && url.pathname === "/_admin/reset") {
    try {
      const body = (await readJson(request)) as { failFirst?: unknown };
      await resetState(body);
      sendJson(response, 200, { service: serviceName, failFirst, reset: true });
    } catch (error) {
      sendJson(response, error instanceof RangeError ? 400 : 500, {
        error: error instanceof Error ? error.message : "Reset failed",
      });
    }
    return;
  }

  const route = url.pathname.match(/^\/workspaces\/([^/]+)\/(resources|billing-profiles)$/);
  const expectedRoute = serviceName === "billing" ? "billing-profiles" : "resources";
  if (request.method !== "POST" || !route || route[2] !== expectedRoute) {
    sendJson(response, 404, { error: "Route not found", service: serviceName });
    return;
  }

  const idempotencyKey = request.headers["idempotency-key"];
  if (typeof idempotencyKey !== "string" || idempotencyKey.length === 0) {
    sendJson(response, 400, { error: "Idempotency-Key header is required" });
    return;
  }

  let requestPayload: unknown;
  try {
    requestPayload = await readJson(request);
  } catch {
    sendJson(response, 400, { error: "Request body must be valid JSON" });
    return;
  }

  const attempt = await recordAttempt(idempotencyKey);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const existingOperation = await findOperation(client, idempotencyKey);
    if (existingOperation) {
      await client.query("COMMIT");
      await updateAttempt(attempt.id, "replayed");
      sendJson(response, 200, { ...existingOperation, replayed: true, attempt: attempt.number });
      return;
    }

    const workspaceId = decodeURIComponent(route[1]);
    const resourceId = `${serviceName}-${encodeURIComponent(route[1])}`;
    const inserted = await client.query<Operation>(
      `INSERT INTO workflow_poc.mock_operations
         (service_name, workspace_id, idempotency_key, resource_id, request_payload)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (service_name, idempotency_key) DO NOTHING
       RETURNING service_name AS service, workspace_id AS "workspaceId",
                 idempotency_key AS "idempotencyKey", resource_id AS "resourceId",
                 created_at AS "createdAt"`,
      [serviceName, workspaceId, idempotencyKey, resourceId, JSON.stringify(requestPayload)],
    );

    if (inserted.rows.length === 0) {
      const racedOperation = await findOperation(client, idempotencyKey);
      await client.query("COMMIT");
      await updateAttempt(attempt.id, "replayed");
      sendJson(response, 200, { ...racedOperation, replayed: true, attempt: attempt.number });
      return;
    }

    if (attempt.number <= failFirst) {
      await client.query("ROLLBACK");
      await updateAttempt(attempt.id, "rolled_back_injected_failure");
      sendJson(response, 503, {
        error: "Injected transient failure; operation transaction rolled back",
        service: serviceName,
        idempotencyKey,
        attempt: attempt.number,
        retryable: true,
      });
      return;
    }

    await client.query("COMMIT");
    await updateAttempt(attempt.id, "committed");
    sendJson(response, 201, { ...inserted.rows[0], replayed: false, attempt: attempt.number });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    await updateAttempt(attempt.id, "rolled_back_error").catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

const server = createServer((request, response) => {
  void handleRequest(request, response).catch((error: unknown) => {
    console.error(`${serviceName} request failed`, error);
    if (!response.headersSent) {
      sendJson(response, 500, { error: "Mock service failed" });
    }
  });
});

async function start(): Promise<void> {
  await initializeDatabase();
  server.listen(port, "0.0.0.0", () => {
    console.log(`${serviceName} mock service listening on port ${port}; failFirst=${failFirst}`);
  });
}

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    server.close(() => void pool.end());
  });
}

void start().catch((error: unknown) => {
  console.error(`${serviceName} failed to start`, error);
  process.exitCode = 1;
});