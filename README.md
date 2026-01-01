# LilSQL v0.8.4_01 — Logging & Undo Engine (Foundation)

![version](https://img.shields.io/badge/version-v0.8.4__01-blue)
![python](https://img.shields.io/badge/python-3.8%2B-yellow)
![status](https://img.shields.io/badge/status-Experimental-orange)

LilSQL v0.8.4_01 introduces a **fully structured logging system** and a **deterministic undo engine**, forming the foundation for safe state recovery and future redo support.

This release focuses on **correctness, explicit behavior, and invariant safety**, not user-facing convenience.

---

## What’s New in v0.8.4_01

### Structured Logging Engine
- Centralized, append-only command logging
- Each successful mutation produces a single immutable log entry
- Logs include:
  - command
  - database & table scope
  - action type
  - before / after snapshots (where applicable)
  - execution phase metadata
- Log format designed explicitly to support undo & future redo

---

###  Undo Engine (Core Implementation)
- Deterministic undo for **all mutating commands**
- Undo restores engine state using logged snapshots, not recomputation
- Supports:
  - CREATE (database, table, rows)
  - DELETE (database, table, columns, rows)
  - UPDATE (database name, table name, column name, row values)
- Undo semantics are **explicit and bounded**
- Undo history collapses after execution (redo not yet implemented)

---

###  Explicit Undo Semantics
- Undo guarantees **state consistency**, not full data recovery
- Destructive operations follow **force-style semantics**
- Only data explicitly logged is recoverable
- Prevents “ghost undo” and timeline paradoxes

---

###  Cursor-Based Undo Control
- Engine maintains an undo counter representing available undo steps
- Each successful mutation increments undo availability
- Executing undo resets undo history (redo planned for later builds)
- Prevents re-undoing already reverted operations

---

###  Invariant-Safe Design
- Undo never depends on runtime state
- All recovery is log-driven
- Schema and row data are always restored to a valid state
- No partial or ambiguous restoration paths

---

## Important Notes
- Recursive / force delete flags are **planned** but not implemented yet
- This release lays the groundwork for:
  - force delete (`-f`)
  - redo support
  - checkpointing
- Logging format is considered **stable** going forward

---

## Versioning Notes
- v0.8.4 is split into incremental internal builds:
  - **v0.8.4_01** — Logging & Undo foundation
  - **v0.8.4_02** — Force delete semantics (planned)
  - **v0.8.4_03** — Redo engine (planned)
- These builds collectively form the v0.8.4 milestone

---

## Status
This release is **experimental** but architecturally complete for its scope.  
It prioritizes correctness and explicit behavior over convenience.
