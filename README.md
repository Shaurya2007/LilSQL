# LilSQL v0.8.3 — Modular Core & Persistent Runtime

![version](https://img.shields.io/badge/version-v0.8.3-blue)
![python](https://img.shields.io/badge/python-3.8%2B-yellow)
![status](https://img.shields.io/badge/status-Experimental-orange)

LilSQL is a lightweight, JSON-powered mini SQL engine built entirely in Python.

It is **not a SQL clone**.  
It is **my interpretation** of a database engine,
designed to be explicit, safe, debuggable, and human-readable.

---

## What’s New in v0.8.3

- Full **command modularization** across CREATE, UPDATE, DELETE, SHOW
- Centralized and safer **state management**
- Improved **error code clarity with execution-phase awareness**
- Runtime storage decoupled from executable location
- Cleaner routing and handler responsibilities
## Core Commands

LilSQL uses a compact, custom syntax:

- `create` — databases, tables, schemas, rows
- `use` — connect to a database
- `leave` — disconnect
- `show` — display table data
- `update` — modify column values (WHERE-based)
- `delete` — delete rows or column values

All commands are **case-insensitive**.

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

### Table Creation
```
create -users id:int,name:string,age:int,active:bool
```

---

### Insert Rows
```
create -users values (1,alice,18,true),(2,bob,20,false)
```

---

### Show Data (WITH WHERE)
```
show -users
show -users (id,name)
show -users where age > 18
show -users (id,name) where active == true
```

Rules:
- Column list is optional
- WHERE filters rows
- Output is auto-formatted

---

### Update Data (WHERE required)
```
update -users values (_,Alice_New) where id == 1
update -users values (_,Anon) where name == alice
update -users values (_,_,25,_) where age < 20
```

Rules:
- `WHERE` is mandatory
- `_` or empty skips a column
- Multiple rows matching WHERE are updated intentionally
- Schema is never modified
- `all` updates every row

---

### Delete Data

#### Literal delete (NO WHERE)
```
delete -users values (1,_)
```

Rules:
- Deletes exactly one matching row
- Multiple matches cause an error
- Used for precise, surgical deletes

---

#### Conditional delete (WITH WHERE)
```
delete -users values name where age > 25
delete -users values id,name where active == false
delete -users values all where id == 5
```

Rules:
- WHERE implies intentional bulk operation
- Column list decides deletion scope
- `all` deletes entire rows
- Partial columns are set to `null`
- Schema is never touched

---

### WHERE Conditions

Supported operators:
```
>   <   >=   <=   ==   !=
```

Supported data types:
- `int`
- `float`
- `bool`
- `string`
- `null`

---

## Folder Structure

```
LilSQL/
│── CMDHandler/
│   ├── create.py
│   ├── update.py
│   ├── delete.py
│   ├── where.py
│   ├── show.py
│   ├── use.py
│   └── error.py
│── state.py
│── router.py
│── cli.py
```

Databases are now stored in LocalAppdata 

---

## Version History

### v0.8.3 (Current)
- Full modular refactor
- Case-insensitive command language
- Centralized state management
- Persistent runtime directory
- Error code phase tagging
- Prepared architecture for undo & logging

### v0.8.2
- WHERE support for SHOW, UPDATE, and DELETE
- Unified condition engine
- Clear and safe mutation semantics
- Ambiguous delete protection
- Column-aware updates and deletes

### v0.8.1
- Error normalization
- State invariant enforcement
- Guard-clause refactor

### v0.8.0
- Initial public release
- Custom SQL-like engine
- CLI interface
- JSON-based storage

---

## Roadmap

### v0.8.x
- Additional WHERE operators (`AND`, `OR`, `LIKE`, `BETWEEN`)
- Internal cleanup and optimization

### v0.9.x
- Constraints
- Schema-level validation

### v1.0
- Full OOP rewrite
