import { mkdirSync } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

type Operation = {
  service: string;
  workspaceId: string;
  idempotencyKey: string;
  resourceId: string;
  requestPayload: unknown;
  createdAt: string;
};

type Attempt = {
  id: number;
  idempotencyKey: string;
  attemptNumber: number;
  outcome: string;
  recordedAt: string;
};

const serviceName = process.env.MOCK_SERVICE_NAME ?? "";
const port = Number(process.env.PORT ?? 4100);
const initialFailFirst = Number(process.env.FAIL_FIRST_N ?? 0);
const dataDirectory = process.env.MOCK_DATA_DIR ?? "/data";

if (!serviceName || !Number.isInteger(initialFailFirst) || initialFailFirst < 0) {
  throw new Error("Set MOCK_SERVICE_NAME and a non-negative integer FAIL_FIRST_N");
}

mkdirSync(dataDirectory, { recursive: true });
const database = new DatabaseSync(join(dataDirectory, `${serviceName}.sqlite`));
database.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000;");
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

function initializeDatabase(): void {
  database.exec(`
    CREATE TABLE IF NOT EXISTS mock_attempt_counters (
      idempotency_key TEXT PRIMARY KEY,
      attempt_count INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS mock_attempts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      idempotency_key TEXT NOT NULL,
      attempt_number INTEGER NOT NULL,
      outcome TEXT NOT NULL DEFAULT 'started',
      recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      UNIQUE (idempotency_key, attempt_number)
    );

    CREATE TABLE IF NOT EXISTS mock_operations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      workspace_id TEXT NOT NULL,
      idempotency_key TEXT NOT NULL UNIQUE,
      resource_id TEXT NOT NULL,
      request_payload TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
  `);
}

function recordAttempt(idempotencyKey: string): { id: number; number: number } {
  database.exec("BEGIN IMMEDIATE");
  try {
    const counter = database
      .prepare(
        `INSERT INTO mock_attempt_counters (idempotency_key, attempt_count)
         VALUES (?, 1)
         ON CONFLICT (idempotency_key)
         DO UPDATE SET attempt_count = mock_attempt_counters.attempt_count + 1
         RETURNING attempt_count`,
      )
      .get(idempotencyKey) as { attempt_count: number };
    const result = database
      .prepare(
        "INSERT INTO mock_attempts (idempotency_key, attempt_number) VALUES (?, ?)",
      )
      .run(idempotencyKey, counter.attempt_count);
    database.exec("COMMIT");
    return { id: Number(result.lastInsertRowid), number: counter.attempt_count };
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  }
}

function updateAttempt(id: number, outcome: string): void {
  database.prepare("UPDATE mock_attempts SET outcome = ? WHERE id = ?").run(outcome, id);
}

function findOperation(idempotencyKey: string): Operation | undefined {
  const row = database
    .prepare(
      `SELECT ? AS service,
              workspace_id AS workspaceId,
              idempotency_key AS idempotencyKey,
              resource_id AS resourceId,
              request_payload AS requestPayload,
              created_at AS createdAt
       FROM mock_operations WHERE idempotency_key = ?`,
    )
    .get(serviceName, idempotencyKey) as Omit<Operation, "requestPayload"> & {
    requestPayload: string;
  } | undefined;

  if (!row) {
    return undefined;
  }

  return { ...row, requestPayload: JSON.parse(row.requestPayload) as unknown };
}

function listState(): { service: string; failFirst: number; attempts: Attempt[]; operations: Operation[] } {
  const attempts = database
    .prepare(
      `SELECT id, idempotency_key AS idempotencyKey, attempt_number AS attemptNumber,
              outcome, recorded_at AS recordedAt
       FROM mock_attempts ORDER BY id`,
    )
    .all() as Attempt[];
  const operationRows = database
    .prepare(
      `SELECT ? AS service, workspace_id AS workspaceId, idempotency_key AS idempotencyKey,
              resource_id AS resourceId, request_payload AS requestPayload, created_at AS createdAt
       FROM mock_operations ORDER BY id`,
    )
    .all(serviceName) as Array<Omit<Operation, "requestPayload"> & { requestPayload: string }>;

  return {
    service: serviceName!,
    failFirst,
    attempts,
    operations: operationRows.map((operation) => ({
      ...operation,
      requestPayload: JSON.parse(operation.requestPayload) as unknown,
    })),
  };
}

function resetState(body: { failFirst?: unknown }): void {
  if (
    body.failFirst !== undefined &&
    (!Number.isInteger(body.failFirst) || Number(body.failFirst) < 0)
  ) {
    throw new RangeError("failFirst must be a non-negative integer");
  }

  database.exec("BEGIN IMMEDIATE");
  try {
    database.exec("DELETE FROM mock_operations; DELETE FROM mock_attempts; DELETE FROM mock_attempt_counters;");
    database.exec("COMMIT");
    failFirst = body.failFirst === undefined ? initialFailFirst : Number(body.failFirst);
  } catch (error) {
    database.exec("ROLLBACK");
    throw error;
  }
}

function upsertOperation(
  idempotencyKey: string,
  workspaceId: string,
  resourceId: string,
  requestPayload: unknown,
): Operation | undefined {
  const result = database
    .prepare(
      `INSERT INTO mock_operations (workspace_id, idempotency_key, resource_id, request_payload)
       VALUES (?, ?, ?, ?)
       ON CONFLICT (idempotency_key) DO NOTHING`,
    )
    .run(workspaceId, idempotencyKey, resourceId, JSON.stringify(requestPayload));

  if (result.changes === 0) {
    return undefined;
  }

  return findOperation(idempotencyKey);
}

function handleRequest(request: IncomingMessage, response: ServerResponse): void {
  void handleRequestAsync(request, response).catch((error: unknown) => {
    console.error(`${serviceName} request failed`, error);
    if (!response.headersSent) {
      sendJson(response, 500, { error: "Mock service failed" });
    }
  });
}

async function handleRequestAsync(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);

  if (request.method === "GET" && url.pathname === "/health") {
    database.prepare("SELECT 1").get();
    sendJson(response, 200, { status: "ok", service: serviceName, database: "sqlite" });
    return;
  }

  if (request.method === "GET" && url.pathname === "/_admin/state") {
    sendJson(response, 200, listState());
    return;
  }

  if (request.method === "POST" && url.pathname === "/_admin/reset") {
    try {
      const body = (await readJson(request)) as { failFirst?: unknown };
      resetState(body);
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

  const attempt = recordAttempt(idempotencyKey);
  database.exec("BEGIN IMMEDIATE");
  try {
    const existingOperation = findOperation(idempotencyKey);
    if (existingOperation) {
      database.exec("COMMIT");
      updateAttempt(attempt.id, "replayed");
      sendJson(response, 200, { ...existingOperation, replayed: true, attempt: attempt.number });
      return;
    }

    const workspaceId = decodeURIComponent(route[1]);
    const resourceId = `${serviceName}-${encodeURIComponent(route[1])}`;
    const operation = upsertOperation(idempotencyKey, workspaceId, resourceId, requestPayload);

    if (!operation) {
      database.exec("COMMIT");
      updateAttempt(attempt.id, "replayed");
      sendJson(response, 200, {
        ...findOperation(idempotencyKey),
        replayed: true,
        attempt: attempt.number,
      });
      return;
    }

    if (attempt.number <= failFirst) {
      database.exec("ROLLBACK");
      updateAttempt(attempt.id, "rolled_back_injected_failure");
      sendJson(response, 503, {
        error: "Injected transient failure; operation transaction rolled back",
        service: serviceName,
        idempotencyKey,
        attempt: attempt.number,
        retryable: true,
      });
      return;
    }

    database.exec("COMMIT");
    updateAttempt(attempt.id, "committed");
    sendJson(response, 201, { ...operation, replayed: false, attempt: attempt.number });
  } catch (error) {
    if (database.isTransaction) {
      database.exec("ROLLBACK");
    }
    updateAttempt(attempt.id, "rolled_back_error");
    throw error;
  }
}

initializeDatabase();
const server = createServer(handleRequest);
server.listen(port, "0.0.0.0", () => {
  console.log(`${serviceName} mock service listening on port ${port}; failFirst=${failFirst}`);
});

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    server.close(() => database.close());
  });
}