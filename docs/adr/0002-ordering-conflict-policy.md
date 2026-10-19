# ADR-0002: Buffer version gaps for a bounded window, then park-and-alert; last-writer-wins is opt-in
Status: accepted
Date: 2026-10-19

## Context
Events for one aggregate carry a dense per-aggregate `version` (1, 2, 3, …). When version *n+2* arrives before
*n+1* we have three choices: wait, apply anyway, or give up. This is a business decision: applying a patch on top
of missing history can produce an order that is "paid" but has no amount. NFR: a gap should usually fill within
seconds; the reconciler runs every 15 minutes.

## Decision
* A gap puts the event in `buffer` with `window_expires = now + buffer_window` (default 300 s).
* When the missing version applies, buffered successors drain in version order inside the same transaction.
* When the window expires (`worker sweep`) the policy decides, configured by `RECONCILER_GAP_POLICY`:
  * **`park_and_alert`** (default): mark the event `parked`, keep it in `buffer`, raise the `parked_events`
    alert. Nothing is applied. The reconciler's next pass heals the aggregate from the source and drains the buffer.
  * **`last_writer_wins`**: apply the lowest buffered version anyway (state patch merged, version jumps). Lower
    versions arriving later are `stale`. The reconciler repairs any divergence caused by the skipped patches.
* Duplicates of a buffered event are harmless: when the gap fills the second copy is classified `duplicate`/`stale`.

## Alternatives considered
* **Reject out-of-order events (let the sender retry).** Pushes ordering to the sender, which cannot be trusted to
  retry in time; better only if the sender guarantees in-order retry.
* **Unbounded buffering.** No policy needed, but an aggregate with a permanently lost event buffers forever and
  memory/rows grow; rejected.
* **Vector clocks.** Needed only for multi-writer aggregates; v1 has a single writer (the sender) so a scalar
  version suffices.

## Consequences
Easy: the replica never contains a state the sender never had (under `park_and_alert`). Hard: an aggregate can be
stale for up to `buffer_window + reconcile_interval`. We are deliberately bad at low-latency convergence after a
*lost* event. Reversibility: two-way door (config).

## Revisit criteria
Reopen if parked-event alerts fire more than once a week (the window is too short or the sender is losing events),
or if a consumer needs freshness under 5 minutes.
