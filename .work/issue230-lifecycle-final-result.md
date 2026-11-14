# Issue #230 lifecycle ownership redesign closure

Status: PASS

- Rejected head: `f22f311f13c0451fb1a14a5ab2b93104efcb8295`
- Fix commit subject: `(fix) close lifecycle ownership redesign blockers`
- Push: not performed

## Closure summary

### B1 — normal Candles stop retains ownership

- `CandlesBase.stop_network()` now snapshots the generation-owned fill task, requests cancellation, drains it to a monotonic hard deadline, and retains a cancellation-suppressing task in residual ownership.
- The current task reference is identity-cleared only by terminal completion; it is never cleared before terminal or residual indexing.
- Repeated and overlapping cleanup calls reuse the exact task and issue at most one cancellation request.

### B2 — exact websocket/generation binding

- Pacifica ping and Candles fill tasks use the same reusable `GenerationOwnedTaskOwner` protocol.
- Each task is mapped by exact websocket object identity when created.
- Interruption hooks resolve the task from their passed websocket instead of reading a mutable current-task field.
- Stale hooks cannot cancel or clear a replacement task. Candles replacement `_ws_candle_available` and `_candles` state is cleared only by the current owning generation.

### B3 — bounded residual ownership and reconnect gating

- Each owner retains at most one live residual task and rejects a second live residual invariant violation.
- Repeated cleanup attempts share the same strong residual reference and done callback.
- Pacifica connection creation is serialized and waits for release; the Candles listener also waits before reconnecting.
- Completion callbacks retrieve failures, prune generation/residual indexes, clear only matching aliases, and permit the next generation to start.

### B4 — explicit primary outcome precedence

- `await_primary_then_cleanup()` captures the disconnect return, ordinary exception, or exact `CancelledError` object and arguments.
- Cleanup runs in a guarded bounded block; cleanup failures are logged and cannot replace the primary outcome.
- Tests cover return, exception, cancellation, timeout, child failure, and explicit cleanup-helper failure for Pacifica and Candles.

### B5 — non-vacuous static enforcement

- The audit still pins the exact 25 expected order-stream definitions and now requires each body to contain its recognized awaited lifecycle boundary.
- Exact alternate boundaries are declared for delegated/polling implementations instead of accepting arbitrary awaits.
- Cleanup recognition includes exact local task cancellation/clearing and reusable cleanup helpers.
- Mutation controls reject an empty boundary set, deletion of the expected hook while unrelated awaits remain, and unsafe exact-local cleanup after disconnect.

## TDD evidence

Before the production redesign, the new reviewer-focused cases failed under actual CPython 3.10 in both modes: `8 failed` normal and `8 failed` under `python -O`. The missing generation binding, residual gating, and static-audit enforcement produced the expected red state.

## Verification

- Focused core/EVEDEX/Pacifica/Candles: `152 passed` normal and `152 passed` optimized.
- Affected lifecycle/generation/Bitmart selection: `166 passed`.
- Gemini/Derive/Derive perpetual/Bitmart connector matrix: `533 passed`.
- Historical reviewer and adjacent lifecycle probes: exit `0` in normal and optimized modes.
  - `reviewer-probes-30391d0.py` SHA-256: `5c50403fb5294d95ae6b11e5661501e81b072579c6f17d8d2698b8435144221f`
  - `adjacent-lifecycle-probes.py` SHA-256: `4c5680c2fbe12502360dd186934f5c2e4cb514948a955f89182709d647ef0a0a`
- Actual CPython 3.10.0 Pacifica plus Candles files: `41 passed` normal and `41 passed` optimized.
- Static 25-definition inventory and three mutation controls: `4 passed` normal and `4 passed` optimized; the full inventory is also included in both focused runs.
- CPython 3.13 forced Cython build/import: `60/60` generated and `60/60` imported.
- CPython 3.10 forced Cython build/import after runtime-specific regeneration: `60/60` generated and `60/60` imported.
- Actual CPython 3.10 parsed and compiled all changed Python files: `7/7`.
- Changed-file newer-asyncio API scan: `0` hits for `TaskGroup`, `Task.cancelling()`, `uncancel()`, or `asyncio.timeout()`.
- Flake8 on all changed Python files: pass.
- `git diff --check`: pass.

The full repository-wide static inventory cannot execute under Python 3.10 because the unchanged Bitmart derivative contains a PEP-701-style f-string at `hummingbot/connector/derivative/bitmart_perpetual/bitmart_perpetual_derivative.py:759`. All changed files parse and compile on 3.10, and the affected lifecycle behavior was exercised under the actual 3.10 runtime rather than only compiled.

## Hygiene

- Removed the build tree, 120 in-place generated `.pyd` files plus their 120 build copies, and all 60 untracked generated Cython `.cpp` files.
- Tracked generated `.cpp` diff: `0` files.
- Pre-existing `.work/` artifacts were preserved; this requested result is the only new report.
