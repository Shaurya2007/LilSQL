import os
import json
import state
from . import error
from logs import log_entrymain

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Creating Database-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#

def parse_create_database(db_name):
    return db_name.strip()


def validate_create_database(db_name):

    if state.curr_db is not None:
        error.errorType("LS_201VD")
        return False

    invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']

    if (db_name == "" or db_name.startswith("-") or any(ch in db_name for ch in invalid_chars)):
        error.errorType("LS_001VD")
        return False

    return True


def resolve_db_root():
    base_dir = state.root_dir
    db_root = os.path.join(base_dir, "Database")

    if not os.path.exists(db_root):
        os.makedirs(db_root)

    return db_root


def validate_db_uniqueness(db_root, db_name):

    for db in os.listdir(db_root):
        if db.lower() == db_name.lower():
            error.errorType("LS_301VD")
            return False

    return True


def execute_create_database(db_root, db_name):
    os.makedirs(os.path.join(db_root, db_name))
    print(f"DATABASE '{db_name}' CREATED.")


def create_database(db_name):

    # PARSE
    db_name = parse_create_database(db_name)

    # VALIDATE
    if not validate_create_database(db_name):
        return

    # RESOLVE ROOT
    db_root = resolve_db_root()

    # VALIDATE UNIQUENESS
    if not validate_db_uniqueness(db_root, db_name):
        return

    # EXECUTE + LOG
    try:
        execute_create_database(db_root, db_name)

    except Exception as e:
        # FAILURE LOG (EXECUTE PHASE)
        log_entrymain({
            "command": f"create database {db_name}",
            "db": db_name,
            "table": None,
            "phase": "EXECUTE",
            "status": "FAILED",
            "action": "CREATE_DATABASE",
            "before": None,
            "after": None,
            "where": None,
            "error": "LS_600EX",
            "error_detail": f"{type(e).__name__}: {str(e)[:200]}"   
        })
        raise

    else:
        # SUCCESS LOG
        log_entrymain({
            "command": f"create database {db_name}",
            "db": db_name,
            "table": None,
            "phase": "EXECUTE",
            "status": "SUCCESS",
            "action": "CREATE_DATABASE",
            "before": None,
            "after": None,
            "where": None
        })


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Creating Tables-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#


valid_types = ["int", "float", "bool", "string", "null"]
invalid_name_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']


def parse_create_table(cmd):
    tb_name = cmd[1][1:].strip()
    raw_schema = " ".join(cmd[2:]).strip()

    if not raw_schema:
        error.errorType("LS_005PR")
        return None, None

    parts = [v.strip() for v in raw_schema.split(",")]
    return tb_name, parts


def parse_schema_parts(parts):
    parsed = []

    for p in parts:
        if ":" not in p:
            error.errorType("LS_005PR")
            return None

        col, dtype = p.split(":", 1)
        col = col.strip()
        dtype = dtype.strip().lower()

        if dtype not in valid_types:
            error.errorType("LS_006PR")
            return None

        parsed.append((col, dtype))

    return parsed


def validate_table_name(tb_name):
    if (tb_name == "" or any(ch in tb_name for ch in invalid_name_chars)):
        error.errorType("LS_002VD")
        return False
    return True


def validate_new_columns(cols):
    seen = set()

    for col, _ in cols:
        if (col == "" or any(ch in col for ch in invalid_name_chars)):
            error.errorType("LS_003VD")
            return False

        low = col.lower()
        if low in seen:
            error.errorType("LS_304VD")
            return False

        seen.add(low)

    return True


def resolve_table_path(tb_name):
    return os.path.join(state.curr_dir, f"{tb_name}.json")


def load_existing_table(tb_path):
    with open(tb_path, "r") as f:
        table = json.load(f)
    return table, table["schema"], table["data"]


def execute_create_new_table(tb_path, cols):
    schema = {}

    for col, dtype in cols:
        schema[col] = dtype

    table = {"schema": schema, "data": []}

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)

    print(f"TABLE '{os.path.basename(tb_path)[:-5]}' CREATED.")


def default_value(dtype):
    if dtype == "int":
        return 0
    if dtype == "float":
        return 0.0
    if dtype == "bool":
        return False
    if dtype == "string":
        return ""
    if dtype == "null":
        return None


def execute_add_columns(schema, data, cols):
    for col, dtype in cols:
        schema[col] = dtype
        for row in data:
            row[col] = default_value(dtype)


def persist_table(tb_path, table):
    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)


def create_table(cmd):

    # PARSE
    tb_name, raw_parts = parse_create_table(cmd)
    if tb_name is None:
        return

    cols = parse_schema_parts(raw_parts)
    if cols is None:
        return

    # VALIDATE
    if not validate_table_name(tb_name):
        return

    if not validate_new_columns(cols):
        return

    # RESOLVE PATH
    tb_path = resolve_table_path(tb_name)

    # CASE 1: CREATE NEW TABLE
    if not os.path.exists(tb_path):
        try:
            execute_create_new_table(tb_path, cols)

        except Exception as e:
            log_entrymain({
                "command": " ".join(cmd),
                "db": state.curr_db,
                "table": tb_name,
                "phase": "EXECUTE",
                "status": "FAILED",
                "action": "CREATE_TABLE",
                "before": None,
                "after": None,
                "where": None,
                "error": "LS_600EX",
                "error_detail": f"{type(e).__name__}: {str(e)}"
            })
            raise

        else:
            log_entrymain({
                "command": " ".join(cmd),
                "db": state.curr_db,
                "table": tb_name,
                "phase": "EXECUTE",
                "status": "SUCCESS",
                "action": "CREATE_TABLE",
                "before": None,
                "after": {col: dtype for col, dtype in cols},
                "where": None
            })
            return

    # CASE 2: ADD COLUMNS TO EXISTING TABLE
    table, schema, data = load_existing_table(tb_path)

    for col, _ in cols:
        if col.lower() in (c.lower() for c in schema):
            error.errorType("LS_306RS")
            return

    before_schema = schema.copy()

    try:
        execute_add_columns(schema, data, cols)
        persist_table(tb_path, table)

    except Exception as e:
        log_entrymain({
            "command": " ".join(cmd),
            "db": state.curr_db,
            "table": tb_name,
            "phase": "EXECUTE",
            "status": "FAILED",
            "action": "ALTER_TABLE_ADD_COLUMNS",
            "before": before_schema,
            "after": None,
            "where": None,
            "error": "LS_600EX",
            "error_detail": f"{type(e).__name__}: {str(e)}"
        })
        raise

    else:
        log_entrymain({
            "command": " ".join(cmd),
            "db": state.curr_db,
            "table": tb_name,
            "phase": "EXECUTE",
            "status": "SUCCESS",
            "action": "ALTER_TABLE_ADD_COLUMNS",
            "before": before_schema,
            "after": schema,
            "where": None
        })

    print(f"ADDED {len(cols)} NEW COLUMN(S) TO TABLE '{tb_name}'.")


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Creating Row Values-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#


def parse_create_rows(cmd):

    if len(cmd) < 4 or cmd[2].lower() != "values":
        error.errorType("LS_100PR")
        return None, None

    tb_name = cmd[1][1:].strip()
    raw = " ".join(cmd[3:]).strip()

    if not raw:
        error.errorType("LS_100PR")
        return None, None

    return tb_name, raw


def parse_row_groups(raw):

    rows_raw = []
    current = ""
    inside = False

    for ch in raw:
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

    parsed = []
    for r in rows_raw:
        parsed.append([v.strip().strip("'").strip('"') for v in r.split(",")])

    return parsed


def load_table_for_insert(tb_name):

    tb_path = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tb_path):
        error.errorType("LS_302")
        return None, None, None

    with open(tb_path, "r") as f:
        table = json.load(f)

    return tb_path, table, table["schema"]


def validate_row_lengths(rows, col_count):

    for r in rows:
        while len(r) < col_count:
            r.append("")

        if len(r) > col_count:
            error.errorType("LS_402VD")
            return False

    return True


def cast_value(dtype, val):

    if val == "_" or val == "":
        return None

    if dtype == "int":
        return int(val)

    if dtype == "float":
        return float(val)

    if dtype == "bool":
        low = val.lower()
        if low in ("true", "1"):
            return True
        if low in ("false", "0"):
            return False
        error.errorType("LS_006")
        raise ValueError

    if dtype == "string":
        return str(val)

    if dtype == "null":
        if val.lower() != "null":
            error.errorType("LS_006")
            raise ValueError
        return None


def execute_insert_rows(table, rows, schema):

    schema_items = list(schema.items())
    inserted = 0

    for vals in rows:
        new_row = {}

        for (col, dtype), val in zip(schema_items, vals):
            try:
                new_row[col] = cast_value(dtype, val)
            except:
                return False

        table["data"].append(new_row)
        inserted += 1

    return inserted


def persist_insert_rows(tb_path, table, inserted, tb_name):

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)

    print(f"INSERTED {inserted} ROW(S) INTO '{tb_name}'.")


def create_columnvalues(cmd):

    # PARSE
    parsed = parse_create_rows(cmd)
    if parsed is None:
        return

    tb_name, raw = parsed

    rows = parse_row_groups(raw)
    if rows is None:
        return

    # LOAD
    tb_path, table, schema = load_table_for_insert(tb_name)
    if tb_path is None:
        return

    schema_items = list(schema.items())

    # VALIDATE
    if not validate_row_lengths(rows, len(schema_items)):
        return

    # EXECUTE + PERSIST + LOG
    try:
        inserted = execute_insert_rows(table, rows, schema)
        if inserted is False:
            return

        persist_insert_rows(tb_path, table, inserted, tb_name)

    except Exception as e:
        log_entrymain({
            "command": " ".join(cmd),
            "db": state.curr_db,
            "table": tb_name,
            "phase": "EXECUTE",
            "status": "FAILED",
            "action": "INSERT",
            "before": None,
            "after": None,
            "where": None,
            "error": "LS_600EX",
            "error_detail": f"{type(e).__name__}: {str(e)}"
        })
        raise

    else:
        log_entrymain({
            "command": " ".join(cmd),
            "db": state.curr_db,
            "table": tb_name,
            "phase": "EXECUTE",
            "status": "SUCCESS",
            "action": "INSERT",
            "before": None,
            "after": inserted,
            "where": None
        })


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Creating Main Func.-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#

def is_db_create(cmd):
    return len(cmd) == 2 and not cmd[1].startswith("-")


def is_row_insert(cmd):
    return (len(cmd) >= 3 and cmd[1].startswith("-") and cmd[2].lower() == "values")


def is_table_create_or_alter(cmd):
    return (len(cmd) >= 2 and cmd[1].startswith("-") and (len(cmd) == 2 or cmd[2].lower() != "values"))


def create_main(cmd):

    if not state.check_state():
        return

    try:
        if is_db_create(cmd):
            create_database(cmd[1])
            return

        if state.curr_db is None:
            error.errorType("LS_200")
            return

        if is_row_insert(cmd):
            create_columnvalues(cmd)
            return

        if is_table_create_or_alter(cmd):
            create_table(cmd)
            return

        error.errorType("LS_000")

    except IndexError:
        error.errorType("LS_100")
