# Idempotent Webhook Reconciler

Webhooks arrive twice, out of order, late, corrupted or never; this service turns that into **exactly-once
effects with zero drift against the sender**, and the chaos harness proves it instead of claiming it.

> **Metric (written before the first commit).**
> *Before:* a naive webhook handler applied to the same chaos streams produces duplicate effects and diverged state.
> *After (target):* 100 % of 1 000 generated delivery permutations converge to the state of a perfect in-order,
> exactly-once delivery; 0 duplicate effects under replay.

## Results

_Filled in from measured runs before the v1.0.0 tag._

## What it does
## Architecture
## Quickstart
## Benchmarks
## Failure modes and runbook
## What it is deliberately bad at
## Extensions (not built)
