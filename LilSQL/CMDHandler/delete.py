import os
import json
import state



def delete_database(cmd):

    db_name = cmd[1].strip()

    if state.curr_db != db_name:
        print("YOU MUST BE USING THE DATABASE TO DELETE IT.")
        return

    if not state.curr_dir or not os.path.exists(state.curr_dir):
        print("DATABASE DOES NOT EXIST.")
        return

    try:
        for file in os.listdir(state.curr_dir):
            os.remove(os.path.join(state.curr_dir, file))

        os.rmdir(state.curr_dir)

        print(f"DATABASE '{db_name}' DELETED.")
        state.curr_db = None
        state.curr_dir = None

    except:
        print("FAILED TO DELETE DATABASE.")




def delete_table(cmd):

    db_name = cmd[1]
    tb_name = cmd[3]

    if state.curr_db != db_name:
        print("YOU MUST BE USING THE DATABASE TO DELETE TABLES.")
        return

    tb_path = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tb_path):
        print("TABLE DOES NOT EXIST.")
        return

    try:
        os.remove(tb_path)
        print(f"TABLE '{tb_name}' DELETED.")

    except:
        print("FAILED TO DELETE TABLE.")




def delete_columns(cmd):

    tb_name = cmd[1][1:].strip()
    tb_path = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tb_path):
        print("TABLE DOES NOT EXIST.")
        return

    if "(" in cmd[2] or ")" in cmd[2]:
        print("INVALID COLUMN NAME PARENTHESES.")
        return

    raw = " ".join(cmd[2:])
    cols = [c.strip() for c in raw.split(",")]

    with open(tb_path, "r") as f:
        table = json.load(f)

    schema = table["schema"]
    data = table["data"]

    delete_list = []

    for c in cols:
        if c == "_" or c == "":
            continue
        if c not in schema:
            print(f"COLUMN '{c}' DOES NOT EXIST.")
            return
        delete_list.append(c)

    if not delete_list:
        print("NO VALID COLUMNS TO DELETE.")
        return

    for c in delete_list:
        schema.pop(c, None)

    for row in data:
        for c in delete_list:
            row.pop(c, None)

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)

    print("COLUMNS DELETED.")




def delete_row_values(cmd):

    tb_name = cmd[1][1:].strip()
    tb_path = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tb_path):
        print("TABLE DOES NOT EXIST.")
        return

    raw = " ".join(cmd[3:]).strip()

    with open(tb_path, "r") as f:
        table = json.load(f)

    schema = table["schema"]
    schema_cols = list(schema.keys())
    schema_types = list(schema.values())
    col_count = len(schema_cols)
    data = table["data"]

    # Delete ALL rows
    if raw == "":
        table["data"] = []
        with open(tb_path, "w") as f:
            json.dump(table, f, indent=4)
        print("ALL ROWS DELETED.")
        return

    # Parse (...) groups
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

    if not parsed:
        print("INVALID VALUE GROUP.")
        return

    for target in parsed:

        if len(target) != col_count:
            print("VALUE COUNT MISMATCH.")
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
                    except:
                        match = False
                        break

                elif dtype == "float":
                    try:
                        if float(row_val) != float(val):
                            match = False
                            break
                    except:
                        match = False
                        break

                elif dtype == "bool":
                    try:
                        row_bool = bool(row_val) if isinstance(row_val, bool) else str(row_val).lower() in ("true", "1")
                        tgt_bool = val.lower() in ("true", "1")
                        if row_bool != tgt_bool:
                            match = False
                            break
                    except:
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

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)

    print("ROW VALUES DELETED.")





def delete_main(cmd):
    try:

        if state.curr_db is None:
            print("NO DATABASE SELECTED.")
            return

        if cmd[1][0] != "-":
            if len(cmd) == 2:
                delete_database(cmd)
                return

            if len(cmd) == 4 and cmd[2].lower() == "values":
                delete_table(cmd)
                return

            print("INVALID COMMAND FORMAT.")
            return

        if len(cmd) >= 3 and cmd[2].lower() != "values":
            delete_columns(cmd)
            return

        if len(cmd) >= 3 and cmd[2].lower() == "values":
            delete_row_values(cmd)
            return

        print("INVALID COMMAND FORMAT.")

    except:
        print("INCOMPLETE COMMAND.")
