import os
import json
import state
import error


def delete_database(cmd):

    # PARSE
    db_name = cmd[1].strip()

    # VALIDATE
    if state.curr_db != db_name:
        error.not_connected()
        return

    if not state.curr_dir or not os.path.exists(state.curr_dir):
        error.errorType("LS_300")
        return

    # EXECUTE
    try:
        for file in os.listdir(state.curr_dir):
            os.remove(os.path.join(state.curr_dir, file))

        os.rmdir(state.curr_dir)

        # PERSIST
        print(f"DATABASE '{db_name}' DELETED.")
        state.curr_db = None
        state.curr_dir = None

    except Exception:
        error.errorType("LS_500")


def delete_table(cmd):

    # PARSE
    db_name = cmd[1]
    tb_name = cmd[3]

    # VALIDATE
    if state.curr_db != db_name:
        error.errorType("LS_200")
        return

    tb_path = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tb_path):
        error.errorType("LS_302")
        return

    # EXECUTE + PERSIST
    try:
        os.remove(tb_path)
        print(f"TABLE '{tb_name}' DELETED.")

    except Exception:
        error.errorType("LS_501")


def delete_columns(cmd):

    # PARSE
    tb_name = cmd[1][1:].strip()
    tb_path = os.path.join(state.curr_dir, f"{tb_name}.json")

    # VALIDATE
    if not os.path.exists(tb_path):
        error.errorType("LS_302")
        return

    if "(" in cmd[2] or ")" in cmd[2]:
        error.errorType("LS_000")
        return

    raw = " ".join(cmd[2:])
    cols = [c.strip() for c in raw.split(",")]

    # EXECUTE
    with open(tb_path, "r") as f:
        table = json.load(f)

    schema = table["schema"]
    data = table["data"]

    delete_list = []

    for c in cols:
        if c == "_" or c == "":
            continue
        if c not in schema:
            error.errorType("LS_305")
            return
        delete_list.append(c)

    if not delete_list:
        error.errorType("LS_502")
        return

    for c in delete_list:
        schema.pop(c, None)

    for row in data:
        for c in delete_list:
            row.pop(c, None)

    # PERSIST
    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)

    print("COLUMNS DELETED.")


def delete_row_values(cmd):

    # PARSE
    tb_name = cmd[1][1:].strip()
    tb_path = os.path.join(state.curr_dir, f"{tb_name}.json")

    # VALIDATE
    if not os.path.exists(tb_path):
        error.errorType("LS_302")
        return

    raw = " ".join(cmd[3:]).strip()

    # EXECUTE
    with open(tb_path, "r") as f:
        table = json.load(f)

    schema = table["schema"]
    schema_cols = list(schema.keys())
    schema_types = list(schema.values())
    col_count = len(schema_cols)
    data = table["data"]

    if raw == "":
        table["data"] = []

        # PERSIST 
        with open(tb_path, "w") as f:
            json.dump(table, f, indent=4)

        print("ALL ROWS DELETED.")
        return

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

    parsed = []
    for r in rows_raw:
        vals = [v.strip().strip('"').strip("'") for v in r.split(",")]
        parsed.append(vals)

    # VALIDATE
    if not parsed:
        error.errorType("LS_004")
        return

    for target in parsed:
        if len(target) != col_count:
            error.errorType("LS_402")
            return

        for index, row in enumerate(data):
            match = True

            for col, dtype, val in zip(schema_cols, schema_types, target):

                if val == "_":
                    continue

                row_val = row[col]

                if dtype == "null":
                    if not (row_val is None and val.lower() == "null"):
                        match = False
                        break

                elif dtype == "int":
                    try:
                        if int(row_val) != int(val):
                            match = False
                            break
                    except Exception:
                        match = False
                        break

                elif dtype == "float":
                    try:
                        if float(row_val) != float(val):
                            match = False
                            break
                    except Exception:
                        match = False
                        break

                elif dtype == "bool":
                    try:
                        row_bool = bool(row_val) if isinstance(row_val, bool) else str(row_val).lower() in ("true", "1")
                        tgt_bool = val.lower() in ("true", "1")
                        if row_bool != tgt_bool:
                            match = False
                            break
                    except Exception:
                        match = False
                        break

                elif dtype == "string":
                    if str(row_val) != val:
                        match = False
                        break

                else:
                    match = False
                    break

            if match:

                all_deleted = True
                for val in target:
                    if val == "_":
                        all_deleted = False
                        break

                if all_deleted:
                    data[index] = None
                else:
                    for col, val in zip(schema_cols, target):
                        if val != "_":
                            row[col] = None

    table["data"] = [r for r in data if r is not None]

    # PERSIST
    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)

    print("ROW VALUES DELETED.")


def delete_main(cmd):

    # VALIDATE
    if not state.check_state():
        return

    try:

        if state.curr_db is None:
            error.errorType("LS_200")
            return

        # PARSE
        if cmd[1][0] != "-":

            if len(cmd) == 2:
                delete_database(cmd)
                return

            if len(cmd) == 4 and cmd[2].lower() == "values":
                delete_table(cmd)
                return

            error.errorType("LS_000")
            return

        # PARSE + VALIDATE
        if len(cmd) >= 3 and cmd[2].lower() != "values":
            delete_columns(cmd)
            return

        if len(cmd) >= 3 and cmd[2].lower() == "values":
            delete_row_values(cmd)
            return

        error.errorType("LS_000")

    except Exception:
        error.errorType("LS_100")

