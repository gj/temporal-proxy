# Workflow authorization extension server example

This example decides each call by the team that owns the Workflow or the schedule. Teams share one Namespace, and a
team owns the Workflow types that begin with its name: `payments` owns `payments.Refund`.

The proxy tells the extension server what each request acts on:

- A request that starts a Workflow has the Workflow ID, the Workflow type and the task queue.
- A request on an existing Workflow or schedule has only an ID.

So the server needs a way to map an ID to its owner. The example has two:

| Approach | How it finds the owner | Cost |
| --- | --- | --- |
| `prefix` | Every Workflow ID and schedule ID begins with `<team>/`. | Every ID must follow the convention. |
| `lookup` | It asks the Temporal Service for the Workflow type behind the ID. | One more call per ID, and a race. |

The policy for each is one short file: [`server/prefix.go`](server/prefix.go) and
[`server/lookup.go`](server/lookup.go).

The caller's team is the bearer token as sent, so `Bearer payments` is the payments team. A real server verifies a
credential and reads the team from it; the [authz example](../authz) shows how.

## Prerequisites

- Go and a checkout of this repository.
- The `temporal` CLI.

## Run

Open four terminals. Start the extension server before the proxy.

Terminal 1 starts the dev server:

```bash
temporal server start-dev
```

Terminal 2, in `examples/workflowauthz`, starts the extension server:

```bash
go run ./server -approach prefix
```

Terminal 3, from the repository root, starts the proxy:

```bash
go run ./cmd/proxy serve -c examples/workflowauthz/config.yaml
```

Terminal 4 sends requests through the proxy. No Worker is needed: the example only starts, signals and terminates.

```bash
export TEMPORAL_ADDRESS=127.0.0.1:7234
as() { echo "--grpc-meta=authorization=Bearer $1"; }
```

## The prefix approach

The payments team starts one of its types under its ID prefix:

```bash
temporal workflow start "$(as payments)" --workflow-id payments/1 --type payments.Refund --task-queue payments
```

Every command below is refused with `caller is not permitted`. The proxy log has the reason.

```bash
# Another team's type.
temporal workflow start "$(as lending)" --workflow-id lending/1 --type payments.Refund --task-queue lending

# Another team's ID prefix.
temporal workflow start "$(as lending)" --workflow-id payments/2 --type lending.Underwrite --task-queue lending
temporal workflow signal "$(as lending)" --workflow-id payments/1 --name poke
temporal workflow terminate "$(as lending)" --workflow-id payments/1

# A signal-with-start signals the workflow that already runs under the ID.
temporal workflow signal-with-start "$(as lending)" --workflow-id payments/1 --type lending.Underwrite \
  --task-queue lending --signal-name poke
```

A schedule follows the same rule. The schedule ID and the Workflow that the schedule starts must both be the team's:

```bash
temporal schedule create "$(as payments)" --schedule-id payments/nightly --interval 24h \
  --workflow-id payments/nightly-run --type payments.Refund --task-queue payments
temporal schedule trigger "$(as payments)" --schedule-id payments/nightly

# Refused.
temporal schedule trigger "$(as lending)" --schedule-id payments/nightly
```

The server sees only the requests that pass through the proxy as a start. A child Workflow that a Worker starts can
have any ID, so a real deployment must also control the IDs that Workflow code sets.

## The lookup approach

Stop the extension server and the proxy. Start the extension server with the other approach, then the proxy again:

```bash
go run ./server -approach lookup
```

Workflow IDs now need no prefix:

```bash
temporal workflow start "$(as payments)" --workflow-id refund-1001 --type payments.Refund --task-queue payments
temporal workflow signal "$(as payments)" --workflow-id refund-1001 --name poke

# Refused: the workflow under refund-1001 has one of the payments team's types.
temporal workflow terminate "$(as lending)" --workflow-id refund-1001
temporal workflow signal-with-start "$(as lending)" --workflow-id refund-1001 --type lending.Underwrite \
  --task-queue lending --signal-name poke
```

### The race in the lookup approach

The server looks up the Workflow type, and the request reaches the Temporal Service afterwards. If another Workflow
takes the ID in between, the request acts on a Workflow the decision was not about. For example:

1. `refund-1001` holds a lending Workflow. The lending team asks to terminate it, and the lookup admits the request.
2. That Workflow closes, and a schedule starts a payments Workflow under `refund-1001`. A start by a schedule or by
   a parent Workflow does not pass through the proxy.
3. The terminate arrives and ends the payments Workflow.

The prefix approach has no lookup, so it has no such race.

## A batch over a query

A batch that lists its Workflows is decided like any other request: the team must own every Workflow in the list. A
batch that selects its Workflows by a query does not list them, so neither approach can find their owners. The example
admits it for the `admin` team alone:

```bash
# Refused.
temporal workflow terminate "$(as payments)" --query "WorkflowType='payments.Refund'" --reason cleanup --yes

temporal workflow terminate "$(as admin)" --query "WorkflowType='payments.Refund'" --reason cleanup --yes
```

## What the example refuses

The server refuses every request that is not on a Workflow or a schedule, such as `temporal workflow list` and a
Worker's polls. A real server needs a rule for each of those as well.
