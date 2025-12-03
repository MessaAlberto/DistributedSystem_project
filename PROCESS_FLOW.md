# Distributed Operation Flows

Each operation is described at two levels:
1. **Logical Flow** – what the system tries to accomplish conceptually.
2. **Practical Flow** – which actor methods exchange which messages in every phase.

Mermaid charts summarize the logical flow, while detailed tables trace the practical execution. A legend at the end reminds what each referenced method does.

## Read

**Logical flow**
- Validate that the coordinator is serving and no conflicting write lock is active.
- Query the responsible replicas until read quorum `R` replies arrive.
- Return the highest version and refresh the coordinator’s local cache. On timeout, notify the client.

```mermaid
flowchart TD
    Client -->|ReadRequest| Node
    Node -->|GetVersionRead (key)| Replicas
    Replicas -->|VersionReadResponse| Node
    Node -->|>= R responses?| Decision
    Decision -->|Yes| Update[Return highest version & refresh local store]
    Decision -->|No (timeout)| Fail[Send OperationFailed to client]
```

**Practical flow**
| Step | Description |
| --- | --- |
| 1 | `Node.onReadRequest` checks `serving` and `writeLocks`. |
| 2 | `getResponsibleNodes` computes replicas; `sendWithRandomDelay` dispatches `GetVersionRead`. |
| 3 | `ReadRequestState` stores the coordinator context plus a descriptive timeout string; Akka schedules `ReadTimeout(detail)`. |
| 4 | Replicas run `onGetVersionRead` and reply with `VersionReadResponse`. |
| 5 | `Node.onVersionReadResponse` aggregates responses; once `R` replies arrive it returns `Client.ReadResponse` or, if `state.client == getSelf()`, treats it as a maintenance read. |
| 6 | `Node.onReadTimeout` logs the detail string; it emits `OperationFailed(detail)` to the client or calls `failMembershipAction(detail)` for maintenance reads. |

## Write

**Logical flow**
- Ensure no concurrent write is running.
- Obtain current versions from replicas until write quorum `W` replies are available.
- Compute the next version, broadcast the update, and acknowledge the client. Abort with `OperationFailed` if quorum is never reached.

```mermaid
flowchart TD
    Client -->|WriteRequest| Node
    Node -->|GetVersionWrite| Replicas
    Replicas -->|VersionWriteResponse| Node
    Node -->|>= W responses?| DecisionW
    DecisionW -->|Yes| NewVersion[Compute new version & broadcast UpdateValue]
    NewVersion -->|Ack (implicit)| Client
    DecisionW -->|No (timeout)| FailW[Send OperationFailed to client & participants]
```

**Practical flow**
| Step | Description |
| --- | --- |
| 1 | `Node.onWriteRequest` verifies `serving` and absence of `writeLocks`. |
| 2 | `getResponsibleNodes` enumerates replicas and `sendWithRandomDelay` delivers `GetVersionWrite`. |
| 3 | `WriteRequestState` keeps quorum responses plus a descriptive timeout string; Akka schedules `WriteTimeout(detail)`. |
| 4 | Replicas execute `onGetVersionWrite`, placing a local lock and answering with `VersionWriteResponse`. |
| 5 | `Node.onVersionWriteResponse` aggregates replies; once `W` are present it sets `state.newVersion` and sends `UpdateValue` to every responsible node. |
| 6 | Client receives `Client.WriteResponse`. If timeout fires, `onWriteTimeout` logs the detail and issues `OperationFailed(detail)` to the client and every contacted replica. |

## Join

**Logical flow**
- A joining node learns the current topology from the bootstrap, requests its key range from the successor, and verifies the data via maintenance reads before announcing itself.
- A single membership timeout covers bootstrap, data transfer, and maintenance phases with descriptive labels.

```mermaid
flowchart TD
    TM[TestManager] -->|Spawn JOIN node| Node
    Node -->|GetPeers| Bootstrap
    Bootstrap -->|PeerResponse| Node
    Node -->|ItemRequest| Successor
    Successor -->|DataItemsBatch| Node
    Node -->|Maintenance Reads (quorum R)| ReplicaSet
    ReplicaSet -->|All reads ok| Node
    Node -->|AnnounceJoin & OK| TM
    Node -. timeout detail .-> TM
```

**Practical flow**
| Phase | Description |
| --- | --- |
| Bootstrap | `preStart` sends `GetPeers`; `scheduleMembershipTimeout("waiting bootstrap…")` arms the timer. |
| Peer view | `onPeerResponse` stores `nodes`/`nodeIdMap` and computes successor. |
| Transfer request | `clearPendingTransfers`; create context via `newTransferContext`; `sendWithRandomDelay(successor, new ItemRequest(..., ctx))`; `scheduleMembershipTimeout("waiting data transfer from successor …")`. |
| Data reception | `onDataItemsBatch` loads items, removes the context, and if all contexts are done cancels the timeout. |
| Maintenance | `startMaintenanceReads` launches per-key reads, sets `requester = getSelf()` (so aggregation never dereferences `null`), and re-arms timeout with key count. |
| Completion | `completeMaintenanceIfDone` cancels timeout, broadcasts `AnnounceJoin`, and reports `NodeActionResponse(detail="Maintenance completed")`. |
| Failure paths | Any timeout or `onReadTimeout` for maintenance invokes `failMembershipAction` with the precise stage detail. |

## Leave

**Logical flow**
- An idle node hands off every key to the appropriate replicas, tells peers that it is leaving, then notifies the manager and stops.

```mermaid
flowchart TD
    TM -->|NodeAction leave| Node
    Node -->|TransferData per key| Successors
    Node -->|AnnounceLeave| Peers
    Node -->|NodeActionResponse (leave, success)| TM
```

**Practical flow**
| Step | Description |
| --- | --- |
| 1 | `Node.onNodeAction(leave)` ensures `mode == IDLE`, switches to `CRASHED`, and copies current `nodes`/`nodeIdMap`. |
| 2 | For each local key, `getResponsibleNodes` (excluding self) decides targets; `sendWithRandomDelay` emits `TransferData`. |
| 3 | `AnnounceLeave` is broadcast so peers prune stale replicas. |
| 4 | `testManager.tell(new NodeActionResponse("leave", …))` confirms success before the actor stops. |

## Recovery

**Logical flow**
- A crashed node re-enters by synchronizing with both successor and predecessor, validating the fetched keys via maintenance reads, and reporting success. Missing neighbors or stalled transfers generate immediate, descriptive failures.

```mermaid
flowchart TD
    TM -->|NodeAction recover| Node
    Node -->|GetPeers| Bootstrap
    Bootstrap -->|PeerResponse| Node
    Node -->|ItemRequest ctx_succ| Successor
    Node -->|ItemRequest ctx_pred| Predecessor
    Successor -->|DataItemsBatch ctx_succ| Node
    Predecessor -->|DataItemsBatch ctx_pred| Node
    Node -->|Maintenance Reads| ReplicaSet
    ReplicaSet -->|Quorum ok| Node
    Node -->|NodeActionResponse (recover, success)| TM
    Node -. stage-specific timeout detail .-> TM
```

**Practical flow**
| Phase | Description |
| --- | --- |
| Trigger | `onNodeAction(recover)` sets mode to `RECOVERING`, clears pending transfers, sends `GetPeers`, and calls `scheduleMembershipTimeout("waiting bootstrap…")`. |
| Neighbor discovery | `onPeerResponse` finds successor/predecessor; if any is missing, `failMembershipAction("Missing neighbor…")`. |
| Dual transfer | For each neighbor, `newTransferContext` + `ItemRequest(..., ctx)`; timeout detail lists every pending peer. |
| Data arrival | `onDataItemsBatch` merges data, removes contexts, and once both batches are in, invokes `pruneKeysNotResponsible` then `startMaintenanceReads`. |
| Maintenance | Same as JOIN: per-key `ReadRequestState`, `requester = getSelf()` to keep coordinator bookkeeping consistent, and timeout detail naming the number of keys. |
| Completion | `completeMaintenanceIfDone` cancels timeout, switches to `IDLE`, and reports success with detail = “Maintenance completed”. |
| Failure paths | Any pending-context timeout or maintenance read timeout calls `failMembershipAction`, which informs the manager and reverts the node to `CRASHED`. |

## Timeout Reporting Summary

- READ/WRITE timeouts now carry descriptive detail strings (`ReadTimeout(detail)`, `WriteTimeout(detail)`) so failures identify the exact key and requester/client.
- JOIN/RECOVER always have at most one active membership timeout, re-armed with the latest textual detail.
- Every outbound transfer stores a context, letting expired timers report exactly which peer failed.
- Maintenance reads reuse the regular read pipeline, set `requester = getSelf()` to avoid null pointers, and detect `state.client == getSelf()` to escalate failures to membership level.

## Method Legend

- `Node.onReadRequest` / `Node.onWriteRequest`: entry points for client operations; perform validation and start quorum gathering.
- `getResponsibleNodes`: determines the replica set for a key based on the ring order.
- `sendWithRandomDelay`: simulates network delay while dispatching messages between actors.
- `startMaintenanceReads`: launches quorum reads for a batch of keys during JOIN/RECOVER and arms the membership timeout.
- `completeMaintenanceIfDone`: finalizes JOIN/RECOVER when no maintenance coordinator remains, canceling timers and notifying the manager.
- `scheduleMembershipTimeout` / `cancelMembershipTimeout`: manage the single descriptive timer that guards JOIN/RECOVER phases.
- `failMembershipAction`: aborts JOIN/RECOVER, reports the precise blocked phase to the `TestManager`, and resets actor state.
- `onDataItemsBatch`: receives transferred data, tracks pending contexts, and triggers maintenance reads when both successor and predecessor have replied.
- `onReadTimeout` / `onWriteTimeout`: fallback handlers that either fail the client or escalate maintenance failures depending on the caller.
