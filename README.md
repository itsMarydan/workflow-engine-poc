# Workflow Engine POC

Compare Inngest and Temporal by implementing the same workspace-provisioning workflow in both. Start with Inngest, then port the behavior to Temporal. The goal is to learn which product fits this orchestration problem better, not to benchmark raw throughput.

## Workflow Under Test

A client asks the **workspace service** to create or provision a workspace. The workspace service owns the workspace API and coordinates three independently deployed service boundaries, in order:

1. **Data service:** provision the workspace's data resources.
2. **Storage service:** create its storage resources.
3. **Billing service:** create its billing profile.

The data, storage, and billing services are separate services with their own APIs. The workspace service coordinates calls to them; it does not reach into their databases or replace their work with in-process function calls. The workspace is reported as provisioned only after all three steps succeed.

Use simulated service implementations for the spike, but preserve the real service boundaries: make calls through the same kind of API each implementation would use in production. Keep a durable operation ledger in the stubs so tests can prove whether a side effect happened more than once.

## Failure And Recovery Scenarios

Run the same scenarios against each engine:

1. **Transient billing failure:** configure billing-profile creation to fail on its first two calls and succeed on the third. Verify the workflow retries billing and does not repeat successful data or storage work.
2. **Application restart:** stop the workspace-service process after at least one downstream step has succeeded, restart it, and verify the workflow completes without repeating completed external side effects.
3. **Durable timer:** include one visible, configurable wait or retry delay. Stop the relevant application process while the workflow is waiting, restart it, and verify the wait resumes without an in-process timer or a repeated completed step.
4. **Failure inspection:** inspect a failed or waiting run in the engine's UI and identify the current step, attempt history, error, and next retry or timer without relying only on application logs.

Use a stable idempotency key based on the workspace and operation when calling each service. Workflow engines can durably track their own steps, but an HTTP side effect can still be duplicated if a process crashes between the service committing its work and the workflow recording the response. The service stubs should honor idempotency keys and expose their operation counts so this distinction is visible.

## Comparison Criteria

Record evidence for each implementation rather than relying on an overall impression:

| Criterion | What to assess |
| --- | --- |
| Implementation effort | Workflow-specific code, service adapters, tests, and configuration, counted separately |
| Microservice integration | How natural it is to call independently deployed services and handle their contracts |
| Failure diagnosis | How quickly the UI and run history explain the failed step, attempts, and recovery state |
| Idempotency | How clearly workflow replay and downstream side-effect deduplication are handled |
| Durable timers | How easy it is to express and reason about waits and delayed retries across restarts |
| Framework friction | How much the implementation must work around framework concepts, SDK constraints, or local tooling |
| Infrastructure and configuration | Required services, persistence setup, secrets, operational knobs, and local startup steps |

Keep the comparison fair: use the same service APIs, deterministic failure controls, idempotency rules, retry policy, wait duration, and restart points. Do not count service-stub code as orchestration code. Record meaningful setup and debugging friction alongside code size; line count alone is not a useful winner metric.

Historically, this kind of process has been handled with NATS JetStream. Use that approach as the reference point for the comparison, especially for explicit message handling, retries, idempotency, and operational visibility. This spike starts with Inngest and Temporal; a third NATS implementation is not required unless the first two leave an important question unanswered.

## Local Infrastructure

The mocks need no shared database. Each service stores its attempt history and committed operations in a local SQLite file under `.data/mock-services/`. The folder is gitignored and bind-mounted into the containers, so the demo data survives container restarts and remains with the project checkout.

```sh
cd /path/to/workflow-engine-poc
docker compose up -d --build
```

Docker Compose is the only runtime prerequisite for the mock services. The containers use Node 22. To run TypeScript directly on the host, use Node 22.5 or newer for the built-in `node:sqlite` module.

- Inngest Dev Server: http://localhost:8288
- Temporal UI: http://localhost:8233
- Temporal gRPC: `localhost:7233`
- Data mock: `http://localhost:4101`
- Storage mock: `http://localhost:4102`
- Billing mock: `http://localhost:4103`

Each mock exposes `GET /health`, `GET /_admin/state`, and `POST /_admin/reset`. The admin routes are intended for this local spike and the service ports bind only to loopback on the host. To configure deterministic billing failures:

```sh
curl -X POST http://localhost:4103/_admin/reset \
	-H 'content-type: application/json' \
	-d '{"failFirst":2}'
```

Write operations require an `Idempotency-Key` header. Data and storage accept `POST /workspaces/:workspaceId/resources`; billing accepts `POST /workspaces/:workspaceId/billing-profiles`. Inspect attempts and committed effects at `/_admin/state`. Each service's `mock_attempts` table retains failed attempts, including `rolled_back_injected_failure`; its `mock_operations` table contains only committed effects and has a unique constraint on idempotency key. This makes the retry, rollback, and deduplication behavior inspectable through the API or directly in that service's SQLite file.

The mock database files are `data.sqlite`, `storage.sqlite`, and `billing.sqlite` in `.data/mock-services/`. The mocks use SQLite WAL mode with full synchronous writes and survive container restarts. The engine containers are still dev-server bootstraps: Inngest does not yet use external Postgres/Redis, and Temporal does not yet use Postgres persistence. Configure those durable engine backends before treating engine restart behavior as evidence for the workflow recovery scenarios above.

## Dependencies

Install the Inngest and Temporal TypeScript SDKs and the TypeScript development tools:

```sh
npm install
```