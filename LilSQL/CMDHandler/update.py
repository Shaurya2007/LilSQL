import os
import json
import state
from . import error
from . import where

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Changing Database Names-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#

def parse_update_db(cmd):

    if len(cmd) < 3 or not cmd[2]:
        error.errorType("LS_100PR")
        return None

    return cmd[2].strip()


def validate_update_db(new_name):

    if state.curr_db is None or state.curr_dir is None:
        error.errorType("LS_200VD")
        return None

    old_path = state.curr_dir
    parent_dir = os.path.dirname(old_path)

    if not os.path.exists(old_path):
        error.errorType("LS_200VD")
        return None

    for db in os.listdir(parent_dir):
        if db.lower() == new_name.lower():
            error.errorType("LS_301VD")
            return None

    return old_path, parent_dir


def execute_update_db(old_path, parent_dir, new_name):

    new_path = os.path.join(parent_dir, new_name)

    os.rename(old_path, new_path)
    state.curr_db = new_name
    state.curr_dir = new_path

    return True


def update_db(cmd):

    # PARSE
    new_name = parse_update_db(cmd)
    if new_name is None:
        return

    # VALIDATE
    result = validate_update_db(new_name)
    if result is None:
        return

    old_path, parent_dir = result

    # EXECUTE
    if not execute_update_db(old_path, parent_dir, new_name):
        return

    # PERSIST
    print("DATABASE RENAMING COMPLETED.")


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Changing Table Names-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#

def parse_update_tablename(cmd):

    raw = " ".join(cmd[3:]).strip()

    if not raw:
        error.errorType("LS_100PR")
        return None

    return [v.strip() for v in raw.split(",")]


def validate_update_tablename(new_names):

    if state.curr_db is None:
        error.errorType("LS_200VD")
        return None

    old_tables = [f[:-5] for f in os.listdir(state.curr_dir) if f.endswith(".json")]

    if len(new_names) > len(old_tables):
        error.errorType("LS_402VD")
        return None

    invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']

    for nm in new_names:
        if nm == "_":
            continue
        if nm == "" or any(ch in nm for ch in invalid_chars):
            error.errorType("LS_002VD")
            return None

    for nm in new_names:
        if nm == "_":
            continue
        for old in old_tables:
            if nm.lower() == old.lower():
                error.errorType("LS_303VD")
                return None

    return old_tables


def execute_update_tablename(old_tables, new_names):

    for i, new_name in enumerate(new_names):

        if new_name == "_":
            continue

        old_name = old_tables[i]

        old_path = os.path.join(state.curr_dir, f"{old_name}.json")
        new_path = os.path.join(state.curr_dir, f"{new_name}.json")

        if not os.path.exists(old_path):
            error.errorType("LS_302EX")
            return False

        os.rename(old_path, new_path)
        print(f"TABLE RENAMED TO '{new_name}'.")

    return True


def update_tablename(cmd):

    # PARSE
    new_names = parse_update_tablename(cmd)
    if new_names is None:
        return

    # VALIDATE
    old_tables = validate_update_tablename(new_names)
    if old_tables is None:
        return

    # EXECUTE
    if not execute_update_tablename(old_tables, new_names):
        return

    # PERSIST
    print("TABLE RENAMING COMPLETED.")


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Changing Column Names-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#

def parse_update_columnname(cmd):

    raw = " ".join(cmd[2:]).strip()

    if not raw:
        error.errorType("LS_100PR")
        return None

    if "(" in raw or ")" in raw:
        error.errorType("LS_003PR")
        return None

    return [v.strip() for v in raw.split(",")]


def validate_update_columnname(new_names):

    invalid_chars = set(['/','\\',':','*','?','"',"'",'<','>','|'])
    cleaned = []
    seen = set()

    for nm in new_names:

        if nm == "_":
            cleaned.append("_")
            continue

        if nm == "" or any(ch in nm for ch in invalid_chars):
            error.errorType("LS_003VD")
            return None

        low = nm.lower()
        if low in seen:
            error.errorType("LS_304VD")
            return None

        seen.add(low)
        cleaned.append(nm)

    return cleaned


def load_table_for_column_update(cmd):

    tb_name = cmd[1][1:].strip()
    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tar_dir):
        error.errorType("LS_302")
        return None, None, None

    with open(tar_dir, "r") as f:
        table = json.load(f)

    return tar_dir, table["schema"], table["data"]


def execute_update_columnname(schema, data, new_names):

    schema_items = list(schema.items())

    if len(new_names) > len(schema_items):
        error.errorType("LS_402EX")
        return None, None

    new_schema = {}
    new_data = []

    for (col, dtype), new_name in zip(schema_items, new_names):
        if new_name == "_":
            new_schema[col] = dtype
        else:
            new_schema[new_name] = dtype

    if len(new_names) < len(schema_items):
        for col, dtype in schema_items[len(new_names):]:
            new_schema[col] = dtype

    for row in data:
        new_row = {}

        for (col, _), new_name in zip(schema_items, new_names):
            if new_name == "_":
                new_row[col] = row[col]
            else:
                new_row[new_name] = row[col]

        if len(new_names) < len(schema_items):
            for col, _ in schema_items[len(new_names):]:
                new_row[col] = row[col]

        new_data.append(new_row)

    return new_schema, new_data


def persist_update_columnname(tar_dir, new_schema, new_data):

    table = {
        "schema": new_schema,
        "data": new_data
    }

    with open(tar_dir, "w") as f:
        json.dump(table, f, indent=4)

    print("COLUMN RENAMING COMPLETED.")


def update_columnname(cmd):

    # PARSE
    new_names = parse_update_columnname(cmd)
    if new_names is None:
        return

    # VALIDATE
    new_names = validate_update_columnname(new_names)
    if new_names is None:
        return

    # LOAD
    tar_dir, schema, data = load_table_for_column_update(cmd)
    if tar_dir is None:
        return

    # EXECUTE
    result = execute_update_columnname(schema, data, new_names)
    if result is None:
        return

    new_schema, new_data = result

    # PERSIST
    persist_update_columnname(tar_dir, new_schema, new_data)


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Changing Row Values-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#

def parse_update_columnvalues(cmd):

    if "where" not in [c.lower() for c in cmd]:
        error.errorType("LS_000PR")
        return None

    wh_index = [i for i, c in enumerate(cmd) if c.lower() == "where"][0]

    raw_values = " ".join(cmd[3:wh_index]).strip()
    where_cmd = cmd[wh_index:]

    if not raw_values:
        error.errorType("LS_103PR")
        return None

    rows_raw = []
    current = ""
    inside = False

    for ch in raw_values:
        if ch == "(":
            inside = True
            current = ""
        elif ch == ")":
            inside = False
            rows_raw.append(current.strip())
        elif inside:
            current += ch

    if not rows_raw:
        error.errorType("LS_004PR")
        return None

    vals = [v.strip().strip("'").strip('"') for v in rows_raw[0].split(",")]

    return vals, where_cmd


def load_update_columnvalues(cmd):

    tb_name = cmd[1][1:].strip()
    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tar_dir):
        error.errorType("LS_302")
        return None, None, None, None

    with open(tar_dir, "r") as f:
        table = json.load(f)

    return tar_dir, table["schema"], table["data"], tb_name


def validate_update_columnvalues(vals, schema_items):

    while len(vals) < len(schema_items):
        vals.append("_")

    if len(vals) > len(schema_items):
        error.errorType("LS_402VD")
        return None

    return vals


def resolve_rows_to_update(where_cmd, schema, data):

    rows = where.where_cmd(where_cmd, schema, data)

    if not rows:
        error.errorType("LS_307RS")
        return None

    return rows


def apply_update_columnvalues(rows_to_update, data, schema_items, vals):

    for row_index in rows_to_update:
        row = data[row_index]

        for (col, dtype), val in zip(schema_items, vals):

            if val == "_" or val == "":
                continue

            try:
                if dtype == "int":
                    row[col] = int(val)

                elif dtype == "float":
                    row[col] = float(val)

                elif dtype == "bool":
                    low = val.lower()
                    if low in ("true", "1"):
                        row[col] = True
                    elif low in ("false", "0"):
                        row[col] = False
                    else:
                        error.errorType("LS_401")
                        return False

                elif dtype == "string":
                    row[col] = str(val)

                elif dtype == "null":
                    if val.lower() != "null":
                        error.errorType("LS_401")
                        return False
                    row[col] = None

                else:
                    error.errorType("LS_401")
                    return False

            except:
                error.errorType("LS_401")
                return False

    return True


def persist_update_columnvalues(tar_dir, table, rows_updated, tb_name):

    with open(tar_dir, "w") as f:
        json.dump(table, f, indent=4)

    print(f"UPDATED {rows_updated} ROW(S) IN '{tb_name}'.")


def update_columnvalues(cmd):

    # PARSE
    parsed = parse_update_columnvalues(cmd)
    if parsed is None:
        return

    vals, where_cmd = parsed

    # LOAD
    tar_dir, schema, data, tb_name = load_update_columnvalues(cmd)
    if tar_dir is None:
        return

    schema_items = list(schema.items())

    # VALIDATE
    vals = validate_update_columnvalues(vals, schema_items)
    if vals is None:
        return

    # WHERE
    rows_to_update = resolve_rows_to_update(where_cmd, schema, data)
    if rows_to_update is None:
        return

    # UPDATE
    if not apply_update_columnvalues(rows_to_update, data, schema_items, vals):
        return

    # PERSIST
    persist_update_columnvalues(
        tar_dir,
        {"schema": schema, "data": data},
        len(rows_to_update),
        tb_name
    )


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-=-=-Main Func.-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#

def is_db_update(cmd):
    return cmd[1][0] != "-" and len(cmd) == 3


def is_tablename_update(cmd):
    return cmd[1][0] != "-" and len(cmd) >= 4 and cmd[2].lower() == "values"


def is_columnname_update(cmd):
    return cmd[1][0] == "-" and len(cmd) >= 3 and cmd[2].lower() != "values"


def is_columnvalue_update(cmd):
    return cmd[1][0] == "-" and len(cmd) >= 4 and cmd[2].lower() == "values"


def update_main(cmd):

    if not state.check_state():
        return

    if state.curr_db is None:
        error.errorType("LS_200")
        return

    if is_db_update(cmd):
        update_db(cmd)
        return

    if is_tablename_update(cmd):
        update_tablename(cmd)
        return

    if cmd[1][0] != "-":
        error.errorType("LS_007")
        return

    if is_columnname_update(cmd):
        update_columnname(cmd)
        return

    if is_columnvalue_update(cmd):
        update_columnvalues(cmd)
        return

    error.errorType("LS_000")
