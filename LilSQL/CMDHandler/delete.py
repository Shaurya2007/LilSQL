import os 
import json
import state
from . import error
from . import where
from logs import log_entrymain

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Deleting Database-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#


def _parse_db_name(cmd):
    return cmd[1].strip()


def _validate_delete_db(db_name):

    if state.curr_db is None or state.curr_dir is None:
        error.errorType("LS_200VD")
        return False

    if state.curr_db.lower() != db_name.lower():
        error.errorType("LS_200VD")
        return False

    if not os.path.exists(state.curr_dir):
        error.errorType("LS_300VD")
        return False

    return True


def _execute_delete_db():
    try:
        for file in os.listdir(state.curr_dir):
            os.remove(os.path.join(state.curr_dir, file))

        os.rmdir(state.curr_dir)
        return True

    except Exception:
        error.errorType("LS_500EX")
        return False


def delete_database(cmd):
    # PARSE
    db_name = _parse_db_name(cmd)

    # VALIDATE
    if not _validate_delete_db(db_name):
        return

    # EXECUTE + LOG
    try:
        if not _execute_delete_db():
            return

    except Exception as e:
        log_entrymain({
            "command": " ".join(cmd),
            "db": db_name,
            "table": None,
            "phase": "EXECUTE",
            "status": "FAILED",
            "action": "DELETE_DATABASE",
            "before": db_name,
            "after": None,
            "where": None,
            "error": "LS_600EX",
            "error_detail": f"{type(e).__name__}: {str(e)}"
        })
        raise

    else:
        log_entrymain({
            "command": " ".join(cmd),
            "db": db_name,
            "table": None,
            "phase": "EXECUTE",
            "status": "SUCCESS",
            "action": "DELETE_DATABASE",
            "before": db_name,
            "after": None,
            "where": None
        })

    # PERSIST
    print(f"DATABASE '{state.curr_db}' DELETED.")
    state.curr_db = None
    state.curr_dir = None



#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Deleting Table-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#


def parse_delete_table(cmd):
    return cmd[1].strip(), cmd[3].strip()


def validate_delete_table(db_name, tb_name):

    if state.curr_db is None:
        error.errorType("LS_200VD")
        return None

    if state.curr_db.lower() != db_name.lower():
        error.errorType("LS_200VD")
        return None

    tb_path = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tb_path):
        error.errorType("LS_302VD")
        return None

    return tb_path


def execute_delete_table(tb_path):
    try:
        os.remove(tb_path)
        return True
    except Exception:
        error.errorType("LS_501EX")
        return False


def persist_delete_table(tb_name):
    print(f"TABLE '{tb_name}' DELETED.")


def delete_table(cmd):

    # PARSE
    db_name, tb_name = parse_delete_table(cmd)

    # VALIDATE
    tb_path = validate_delete_table(db_name, tb_name)
    if tb_path is None:
        return

    # EXECUTE + LOG
    try:
        if not execute_delete_table(tb_path):
            return

    except Exception as e:
        log_entrymain({
            "command": " ".join(cmd),
            "db": db_name,
            "table": tb_name,
            "phase": "EXECUTE",
            "status": "FAILED",
            "action": "DELETE_TABLE",
            "before": tb_name,
            "after": None,
            "where": None,
            "error": "LS_600EX",
            "error_detail": f"{type(e).__name__}: {str(e)}"
        })
        raise

    else:
        log_entrymain({
            "command": " ".join(cmd),
            "db": db_name,
            "table": tb_name,
            "phase": "EXECUTE",
            "status": "SUCCESS",
            "action": "DELETE_TABLE",
            "before": tb_name,
            "after": None,
            "where": None
        })

    # PERSIST
    persist_delete_table(tb_name)


#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Deleting Columns-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#

def parse_delete_columns(cmd):
    tb_name = cmd[1][1:].strip()
    raw = " ".join(cmd[2:]).strip()

    if "(" in raw or ")" in raw:
        error.errorType("LS_000PR")
        return None, None

    cols = [c.strip() for c in raw.split(",")]

    return tb_name, cols


def validate_delete_columns(tb_name, cols):

    tb_path = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tb_path):
        error.errorType("LS_302VD")
        return None

    if not cols:
        error.errorType("LS_502VD")
        return None

    return tb_path


def load_table_for_column_delete(tb_path):

    with open(tb_path, "r") as f:
        table = json.load(f)

    return table, table["schema"], table["data"]


def resolve_columns_to_delete(cols, schema):

    delete_list = []

    for c in cols:
        if c == "_" or c == "":
            continue

        match = None
        for col in schema:
            if col.lower() == c.lower():
                match = col
                break

        if match is None:
            error.errorType("LS_305RS")
            return None

        delete_list.append(match)

    if not delete_list:
        error.errorType("LS_502RS")
        return None

    return delete_list


def execute_delete_columns(schema, data, delete_list):

    for c in delete_list:
        schema.pop(c, None)

    for row in data:
        for c in delete_list:
            row.pop(c, None)


def persist_delete_columns(tb_path, table):

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)

    print("COLUMNS DELETED.")


def delete_columns(cmd):

    # PARSE
    tb_name, cols = parse_delete_columns(cmd)
    if tb_name is None:
        return

    # VALIDATE
    tb_path = validate_delete_columns(tb_name, cols)
    if tb_path is None:
        return

    # LOAD
    table, schema, data = load_table_for_column_delete(tb_path)

    # RESOLVE
    delete_list = resolve_columns_to_delete(cols, schema)
    if delete_list is None:
        return

    before_schema = schema.copy()

    # EXECUTE + LOG
    try:
        execute_delete_columns(schema, data, delete_list)
        persist_delete_columns(tb_path, table)

    except Exception as e:
        log_entrymain({
            "command": " ".join(cmd),
            "db": state.curr_db,
            "table": tb_name,
            "phase": "EXECUTE",
            "status": "FAILED",
            "action": "DELETE_COLUMNS",
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
            "action": "DELETE_COLUMNS",
            "before": before_schema,
            "after": schema,
            "where": None
        })



#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Deleting Rows-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#


def parse_delete_row_values(cmd):
    tb_name = cmd[1][1:].strip()
    raw = " ".join(cmd[3:]).strip()
    return tb_name, raw


def validate_delete_row_values(tb_name):
    tb_path = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tb_path):
        error.errorType("LS_302VD")
        return None

    return tb_path


def load_table_for_row_delete(tb_path):
    with open(tb_path, "r") as f:
        table = json.load(f)

    return table, table["schema"], table["data"]

# =-=-=-=-=-=-=-=-=- WHERE MODE =-=-=-=-=-=-=-=-=

def resolve_where_delete(raw, schema, data):
    wh = raw.split()
    wh_index = [i for i, c in enumerate(wh) if c.lower() == "where"][0]

    col_raw = " ".join(wh[:wh_index]).strip()
    condition_cmd = wh[wh_index:]

    rows = where.where_cmd(condition_cmd, schema, data)

    if not rows:
        error.errorType("LS_307RS")
        return None, None

    if col_raw == "":
        error.errorType("LS_000RS")
        return None, None

    cols = [c.strip() for c in col_raw.split(",")]
    schema_cols = list(schema.keys())

    if "all" in [c.lower() for c in cols]:
        if len(cols) != 1:
            error.errorType("LS_000")
            return None, None
        return rows, schema_cols

    delete_cols = []

    for c in cols:
        if c == "" or c == "_":
            continue

        match = None
        for col in schema_cols:
            if col.lower() == c.lower():
                match = col
                break

        if match is None:
            error.errorType("LS_305RS")
            return None, None

        delete_cols.append(match)

    if not delete_cols:
        error.errorType("LS_000RS")
        return None, None

    return rows, delete_cols


def execute_where_delete(data, schema_cols, rows, delete_cols):
    for i in rows:
        if len(delete_cols) == len(schema_cols):
            data[i] = None
        else:
            for c in delete_cols:
                data[i][c] = None

# ================= NO WHERE MODE =================

def parse_literal_rows(raw):
    rows_raw = []
    inside = False
    current = ""

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
        parsed.append([v.strip().strip('"').strip("'") for v in r.split(",")])

    return parsed


def resolve_literal_delete(parsed, schema, data):
    schema_cols = list(schema.keys())
    rows_to_delete = []

    for target in parsed:
        if len(target) != len(schema_cols):
            error.errorType("LS_402RS")
            return None

        for index, row in enumerate(data):
            match = True

            for col, dtype, val in zip(schema_cols, schema.values(), target):

                if val == "_":
                    continue

                try:
                    row_val = row[col]

                    if dtype == "int" and int(row_val) != int(val):
                        match = False
                    elif dtype == "float" and float(row_val) != float(val):
                        match = False
                    elif dtype == "bool":
                        if (str(row_val).lower() in ("true", "1")) != (val.lower() in ("true", "1")):
                            match = False
                    elif dtype == "string" and str(row_val) != val:
                        match = False
                    elif dtype == "null" and not (row_val is None and val.lower() == "null"):
                        match = False
                except:
                    match = False

                if not match:
                    break

            if match:
                rows_to_delete.append(index)

    if len(rows_to_delete) > 1:
        error.errorType("LS_308RS")
        return None

    if not rows_to_delete:
        error.errorType("LS_307RS")
        return None

    return rows_to_delete


def delete_row_values(cmd):

    # PARSE
    tb_name, raw = parse_delete_row_values(cmd)

    # VALIDATE
    tb_path = validate_delete_row_values(tb_name)
    if tb_path is None:
        return

    # LOAD
    table, schema, data = load_table_for_row_delete(tb_path)
    schema_cols = list(schema.keys())

    # WHERE MODE
    if "where" in raw.lower():
        rows, delete_cols = resolve_where_delete(raw, schema, data)
        if rows is None:
            return

        before_rows = [data[i].copy() for i in rows if data[i] is not None]

        try:
            execute_where_delete(data, schema_cols, rows, delete_cols)
            table["data"] = [r for r in data if r is not None]

            with open(tb_path, "w") as f:
                json.dump(table, f, indent=4)

        except Exception as e:
            log_entrymain({
                "command": " ".join(cmd),
                "db": state.curr_db,
                "table": tb_name,
                "phase": "EXECUTE",
                "status": "FAILED",
                "action": "DELETE_ROWS_WHERE",
                "before": before_rows,
                "after": None,
                "where": raw,
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
                "action": "DELETE_ROWS_WHERE",
                "before": before_rows,
                "after": None,
                "where": raw
            })

        print("VALUES DELETED.")
        return

    # NO WHERE MODE (DELETE ALL)
    if raw == "":
        before_rows = table["data"].copy()

        try:
            table["data"] = []
            with open(tb_path, "w") as f:
                json.dump(table, f, indent=4)

        except Exception as e:
            log_entrymain({
                "command": " ".join(cmd),
                "db": state.curr_db,
                "table": tb_name,
                "phase": "EXECUTE",
                "status": "FAILED",
                "action": "DELETE_ALL_ROWS",
                "before": before_rows,
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
                "action": "DELETE_ALL_ROWS",
                "before": before_rows,
                "after": None,
                "where": None
            })

        print("ALL ROWS DELETED.")
        return

    # LITERAL DELETE
    parsed = parse_literal_rows(raw)
    if parsed is None:
        return

    rows = resolve_literal_delete(parsed, schema, data)
    if rows is None:
        return

    before_row = data[rows[0]].copy()

    try:
        data[rows[0]] = None
        table["data"] = [r for r in data if r is not None]

        with open(tb_path, "w") as f:
            json.dump(table, f, indent=4)

    except Exception as e:
        log_entrymain({
            "command": " ".join(cmd),
            "db": state.curr_db,
            "table": tb_name,
            "phase": "EXECUTE",
            "status": "FAILED",
            "action": "DELETE_ROW_LITERAL",
            "before": [before_row],
            "after": None,
            "where": raw,
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
            "action": "DELETE_ROW_LITERAL",
            "before": [before_row],
            "after": None,
            "where": raw
        })

    print("ROW VALUES DELETED.")

#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-==-=-Deleting Main Func.-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#

def is_db_delete(cmd):
    return (len(cmd) == 2 and not cmd[1].startswith("-"))


def is_table_delete(cmd):
    return (len(cmd) == 4 and not cmd[1].startswith("-") and cmd[2].lower() == "values")


def is_column_delete(cmd):
    return (len(cmd) >= 3 and cmd[1].startswith("-") and cmd[2].lower() != "values")


def is_row_delete(cmd):
    return (len(cmd) >= 3 and cmd[1].startswith("-") and cmd[2].lower() == "values")


def delete_main(cmd):

    if not state.check_state():
        return

    if state.curr_db is None:
        error.errorType("LS_200")
        return

    try:
        if is_db_delete(cmd):
            delete_database(cmd)
            return

        if is_table_delete(cmd):
            delete_table(cmd)
            return

        if is_column_delete(cmd):
            delete_columns(cmd)
            return

        if is_row_delete(cmd):
            delete_row_values(cmd)
            return

        error.errorType("LS_000")

    except Exception:
        error.errorType("LS_100")
