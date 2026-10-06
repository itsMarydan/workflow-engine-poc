# Temporal vs Inngest Comparison

## Scope

This document compares the same workflow implemented in both systems:

- Provision a workspace
- Call three downstream services in order:
  1. data
  2. storage
  3. billing
- Preserve idempotency keys per request and service
- Use deterministic failure injection in the billing mock
- Keep the service contract identical between both implementations

The test is intentionally fair: both engines invoke the same HTTP services, use the same request payload, and rely on the same retry semantics.

---

## Test setup

### Shared service contract

Each workflow sends the same payload shape:

```ts
{
  requestId: string,
  workspaceId: string,
  engines: ["inngest"] | ["temporal"]
}
```

The workflow then calls the same downstream endpoints:

- data service: `POST /workspaces/:workspaceId/resources`
- storage service: `POST /workspaces/:workspaceId/resources`
- billing service: `POST /workspaces/:workspaceId/billing-profiles`

The service stubs record all attempts and committed operations in SQLite so we can inspect:

- failed attempts
- replayed calls
- committed side effects
- idempotency-key deduplication

### Local infrastructure

The project uses Docker Compose for the mock services and the Temporal dev server.

- Inngest dev server: `http://localhost:8288`
- Temporal dev server: `localhost:7233`
- Temporal UI: `http://localhost:8233`
- Mock services:
  - data: `http://localhost:4101`
  - storage: `http://localhost:4102`
  - billing: `http://localhost:4103`

---

## Simulation executed

### 1) Inngest simulation

Command used:

```sh
cd /Users/e9004824/workspace/learning/workflow-engine-poc
INNGEST_DEV=1 INNGEST_EVENT_KEY=local-test-key INNGEST_BASE_URL=http://127.0.0.1:8288 npm run inngest:trigger
```

Observed result:

```text
Inngest event sent
```

This means the local Inngest dev server accepted the event and the workflow was scheduled.

### 2) Temporal simulation

Command used:

```sh
cd /Users/e9004824/workspace/learning/workflow-engine-poc
npm run temporal:trigger
```

Observed result:

```text
Temporal workflow started: workspace-provision-demo-request-temporal-001
```

This confirms the Temporal workflow was started on the `workspace-provisioning` task queue and accepted by the worker.

---

## Evidence from downstream services

The mock services were checked after both runs. The results show both implementations executed the same steps and persisted committed work using the same idempotency keys.

### Data service

Observed state included:

- `demo-request-002:data` committed for Inngest
- `demo-request-temporal-001:data` committed for Temporal

### Storage service

Observed state included:

- `demo-request-002:storage` committed for Inngest
- `demo-request-temporal-001:storage` committed for Temporal

### Billing service

Observed state included:

- `demo-request-002:billing` with attempts 1 and 2 failing, then attempt 3 committed for Inngest
- `demo-request-temporal-001:billing` with attempts 1 and 2 failing, then attempt 3 committed for Temporal

This is the key proof point: both engines behaved consistently under the same fail-first billing policy and both preserved the idempotency-key logic.

---

## Comparison analysis

### 1) Mental model

#### Inngest

The workflow is event-driven and function-centric. The orchestration logic is defined as an event-triggered function with steps and sleeps. It feels natural for background event processing and app-level automation.

Pros:

- Very low ceremony for event-triggered tasks
- Easy to reason about if the process is fundamentally "run after an event"
- Good fit for fan-out and asynchronous tasks
- Lightweight local dev experience

Cons:

- The workflow is more of a function with durable step tracking than a strict long-running state machine
- The orchestration semantics are less explicit when the process spans many real-world workflow states
- The workflow history is less “first-class” than Temporal’s runtime model when the team wants stronger workflow semantics

#### Temporal

The workflow is a durable execution model. The runtime tracks the workflow state, timers, retries, and activity execution in a more explicit way. The code reads more like an orchestrator-state machine than an event handler.

Pros:

- Stronger workflow semantics and clearer long-running execution model
- Better fit when the workflow is a business process with explicit milestones
- Durable history is first-class and easier to reason about in strong operational terms
- More natural modeling for long-running operations, retries, cancellations, and restart recovery

Cons:

- More ceremony and more framework-specific concepts to learn
- The developer experience is heavier than Inngest for simple background jobs
- Local setup is a bit more involved because you need both the Temporal server and the worker

### 2) Retry and failure handling

Both implementations handled the fail-first billing workflow the same way:

- attempt 1: failed
- attempt 2: failed
- attempt 3: succeeded

This demonstrates that the same retry policy and the mock service’s idempotency key semantics are visible in both engines.

The important distinction is not whether one engine can retry more than the other. The real difference is how the engine presents and records the state:

- Inngest exposes this as step-level run history and event-driven function progress
- Temporal exposes this as workflow history, activity retries, and execution state transitions

For operational diagnosis, Temporal is more explicit about workflow progression and failure state.

### 3) Idempotency and side effects

This project was designed to make the distinction between workflow replay and side-effect deduplication visible.

The mock services enforce idempotency by keying on:

```text
<requestId>:<service>
```

Example:

```text
demo-request-temporal-001:data
demo-request-temporal-001:storage
demo-request-temporal-001:billing
```

The service implementations keep both:

- `attempts` table: failed and replayed invocations
- `operations` table: only committed side effects

This is a very good pattern because it separates:

- workflow retry behavior
- service-level duplication protection

In both engines, the idempotency contract behaved correctly: the same request did not create duplicate committed records when retried.

### 4) Durable timers and restart recovery

The workflow intentionally includes a delay before the billing step. This matters because the comparison is not only about “did the steps run,” but also about whether the engine can durably resume the process after a restart.

In this repo, the shared flow has a delay in both engines and the service layer records the exact retry and replay history. Temporal has a stronger first-class sense of workflow timers and long-lived execution state. Inngest is still effective, but the stronger business-process semantics are more explicit in Temporal.

### 5) Operational visibility

#### Inngest

Best for:

- quick inspections in a local dev UI
- event-driven workflows
- fast iteration and lower ceremony

Weaknesses:

- the flow still feels like a background job more than a durable workflow engine in the classic sense
- operational debugging is less structural if the process becomes long-running and stateful

#### Temporal

Best for:

- process-heavy orchestration
- auditability
- explicit workflow state transitions
- handling long-running operations with durable timers and recovery semantics

Weaknesses:

- more infrastructure around worker and server setup
- more code and runtime concepts that the team must absorb

### 6) Developer workflow and implementation effort

For this project, the implementation effort was intentionally kept close by sharing the same request model and same HTTP client contract.

The difference is still visible:

- Inngest: faster to wire up, less setup, easier to start iterating
- Temporal: more upfront work, but stronger process semantics and clearer operational model

This means that the decision is not simply “which is better”; it depends on whether the workflow is more like a background job or a business process.

---

## Verdict

### Choose Inngest when:

- the work is triggered by events
- the workflow is relatively short-lived
- simplicity of dev experience matters most
- the team wants a lightweight orchestration model
- the operation is closer to app automation than to formal business workflow execution

### Choose Temporal when:

- the process is a real workflow with retries, explicit state, long-running timers, and process-level durability
- operations need stronger auditability and clearer workflow history
- the team expects long-running business orchestration, not just async functions
- recovery semantics and state inspection are more important than minimal setup

### For this POC

For the exact workflow in this repository, both engines proved the same behavior under the same service contract and hidden failure model. The stronger distinction is in how they model and present the process:

- Inngest is simpler and more ergonomic
- Temporal is more explicit and operationally robust for long-lived workflow semantics

If the team is trying to compare the two as tools for similar business orchestration problems, Temporal wins on workflow clarity and operational rigor. Inngest wins on developer velocity and simplicity.

---

## Final takeaway

This test demonstrates that the difference between Inngest and Temporal is not just syntax. It is the difference between:

- a lightweight event-driven orchestration model
- and a durable workflow execution engine with stronger lifecycle semantics

For this workload, both are capable of handling the same core flow, but Temporal is the better fit when the workflow is treated as a business process, while Inngest is the better fit when the workflow is treated as app-level event processing.
