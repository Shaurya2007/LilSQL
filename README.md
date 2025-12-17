# LilSQL v0.8.1 — A Tiny, File-Based SQL Engine

![version](https://img.shields.io/badge/version-v0.8.1-blue)
![python](https://img.shields.io/badge/python-3.8%2B-yellow)
![status](https://img.shields.io/badge/status-Experimental-orange)

LilSQL is a lightweight, JSON-powered mini SQL engine built entirely in Python.  
Its goal is simple: **SQL-like power without SQL-like complexity.**

LilSQL is **not a clone of SQL**.  
It is **my interpretation** of how a database system works internally.

---

## What’s New in v0.8.1

This release focuses on **internal correctness, safety, and maintainability**.  
No new features were added.

### Improvements
- Centralized error normalization with error codes
- Deterministic database storage location
- Enforced state invariants
- Guard-clause–based control flow
- Safer filesystem handling for `.py` and `.exe`
- Internal refactors preparing for future updates

---

## Core Commands

LilSQL operates using a custom, minimal syntax:

- `create` — databases, tables, schemas, rows
- `update` — rename and modify data
- `delete` — rows, columns, tables, databases
- `show` — display table data
- `use` — connect to a database
- `leave` — disconnect

---

## Features

### Database Management
```
create mydb
use mydb
leave
delete mydb
```

---

### Table & Column Management
```
create -users id:int,name:string
update -users name,_
delete -users name
```

`_` skips a column.

---

### Row Operations
```
create -users values (1,alice),(2,bob)
update -users values (alice_new,_)
delete -users values (1,_)
```

Supported types:
- `int`
- `float`
- `bool`
- `string`
- `null`

---

### Display
```
show -users
show -users (id,name)
```

Pretty-printed output with auto column sizing.

---

## Folder Structure

```
LilSQL/
│── CMDHandler/
│   ├── create.py
│   ├── update.py
│   ├── delete.py
│   ├── use.py
│   ├── show.py
│   └── error.py
│── state.py
│── router.py
│── cli.py
```

Databases are created inside an internal `Database/` directory at runtime.

---

## Version History

### v0.8.1 (Current)
- Error normalization system
- State invariant enforcement
- Deterministic storage
- Guard-clause refactor
- Safer execution
- No feature changes

### v0.8.0
- Initial public release
- Custom SQL-like engine
- CLI interface
- JSON-based storage

---

## Roadmap

### v0.8.x
- Incremental query features (WHERE, LIKE, AND / OR)
- Continued internal optimization

### v0.9.x
- Constraints
- Schema-level validation

### v1.0
- Full OOP rewrite
---