
# LilSQL v0.8 — A Tiny, File-Based SQL Engine

![version](https://img.shields.io/badge/version-v0.8.0-blue)
![python](https://img.shields.io/badge/python-3.8%2B-yellow)
![status](https://img.shields.io/badge/status-Experimental-orange)

LilSQL is a lightweight, JSON-powered mini SQL engine built entirely in Python.
Its goal is simple: **SQL-like power without SQL-like complexity.**

LilSQL operates using a clean custom syntax:
- `create` to build databases, tables, schemas, and rows
- `update` to modify columns and rows
- `delete` to remove columns, rows, tables, or databases
- `show` to display table data
- `use` to connect to a database
- `leave` to disconnect

---

## Features (v0.8)

### Database Management
- `create dbname`
- `use dbname`
- `delete dbname` (must be connected to delete)

### Table Management
- `create -tb col1:type,col2:type`( '-' is used before table name when referencing it)
- `delete dbname values tbname` removes a table

### Column Operations
- `create -tb col1:type,col2:type`
- `update -tb col1,_,col3`
- `delete -tb col1,_,col3`

### Row Operations
- `create -tb values (1,"a"),(2,"b")`
- `update -tb values (new,new,new)`
- `_` skips a column
- Smart type casting (`int`, `float`, `bool`, `string`, `null`)

### Row Deletion
- `delete -tb values (1,_,_)`
- `_` wildcard for matching
- Deletes full row if all match
- Deletes individual values by setting to `null`

### Displaying Data
- `show -tb`
- `show -tb (col1,col2)`
- Pretty formatted output

---

## Folder Structure

```
LilSQL/
│── CMDHandler/
│   ├── create.py
│   ├── update.py
│   ├── delete.py
│   ├── use.py
│   └── show.py
│── state.py
│── router.py
│── cli.py
```

---

## Syntax Reference

### Create
```
create mydb
create -users id:int,name:string    (No need to use mandotory "" as it will be there by default)
create -users values (1,a),(2,b)  
```

### Update
```
update mydb newname
update -users name,_
update -users values (newname,_)
```

### Delete
```
delete mydb
delete mydb values users
delete -users col1,_,col3
delete -users values (1,_,_)
```

### Show
```
show -users
show -users (id,name)
```

### Use Database
```
use mydb
leave
```

---

## Future updates

### v0.9.x
- WHERE support
- Row filtering
- ORDER BY
- LIMIT
- More advanced selection
- Better error messages

### v1.0
- Full OOP rewrite 
- Overall more optimized code

---

## Author Notes
I’m a first-sem BCA student who learned Python barely two weeks before creating LilSQL. 
This project started as “I’m too lazy to type 50 INSERT statements,” and somehow turned into a full custom database engine. Although the problem of inserting 50 statements by hand still remains (talking about dummy data here), I will think of a solution.
The code might be janky, unoptimized, and occasionally behave weird, but it works. 
LilSQL will evolve as I level up so I cannot say that LilSQL will get updates frequently or consistently. Thanks for trying LilSQL out.

