import os
import json
import state
from . import error
from . import where


def update_db(cmd):

    # PARSE
    old_path = state.curr_dir
    new_path = os.path.join(os.path.dirname(state.curr_dir), cmd[2])

    # VALIDATE
    if state.curr_db is None or state.curr_dir is None:
        error.errorType("LS_200")
        return

    if len(cmd) < 3 or not cmd[2]:
        error.errorType("LS_100")
        return
    
    if not os.path.exists(old_path):
        error.errorType("LS_200")
        return

    # PERSIST
    os.rename(old_path, new_path)
    print(f"DATABASE RENAMED TO '{cmd[2]}'.")
    state.curr_db = cmd[2]
    state.curr_dir = new_path


def update_tablename(cmd):

    # PARSE
    old_tb_names = [table[:-5] for table in os.listdir(state.curr_dir) if table.endswith(".json")]
    curr_name = " ".join(cmd[3:]).strip()
    new_tb_names = [v.strip() for v in curr_name.split(",")]
    invalid_name = ['/','\\',':','*','?','"','<','>','|']

    # VALIDATE
    if state.curr_db is None:
        error.errorType("LS_200")
        return

    if len(old_tb_names) < len(new_tb_names):
        error.errorType("LS_402")
        return

    for nm in new_tb_names:
        if nm == "_":
            continue
        if any(ch in nm for ch in invalid_name) or nm == "":
            error.errorType("LS_002")
            return

    # EXECUTE + PERSIST
    for i in range(len(new_tb_names)):
        new_name = new_tb_names[i]
        old_name = old_tb_names[i]

        if new_name == "_":
            continue 

        old_path = os.path.join(state.curr_dir, f"{old_name}.json")
        new_path = os.path.join(state.curr_dir, f"{new_name}.json")

        if not os.path.exists(old_path):
            error.errorType("LS_302")
            return

        if os.path.exists(new_path):
            error.errorType("LS_303")
            return

        os.rename(old_path, new_path)
        print(f"TABLE RENAMED TO '{new_name}'.")

    print("TABLE RENAMING COMPLETED.")


def update_columnname(cmd):

    # PARSE
    raw_values = " ".join(cmd[2:])
    new_col_names = [v.strip() for v in raw_values.split(",")]
    invalid_chars = set(['/','\\',':','*','?','"',"'",'<','>','|'])
    cleaned = []
    seen = set()

    # VALIDATE
    if "(" in raw_values or ")" in raw_values:
        error.errorType("LS_003")
        return
    
    for nm in new_col_names:
        nm = nm.strip()

        if nm == "_":
            cleaned.append("_")
            continue

        if nm == "" or any(ch in nm for ch in invalid_chars):
            error.errorType("LS_003")
            return

        if nm in seen:
            error.errorType("LS_304")
            return

        seen.add(nm)
        cleaned.append(nm)

    new_col_names = cleaned

    tb_name = cmd[1][1:].strip()
    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tar_dir):
        error.errorType("LS_302")
        return

    # EXECUTE
    with open(tar_dir, 'r') as f:
        table = json.load(f)

    schema = table["schema"]
    data = table["data"]
    schema_items = list(schema.items()) 

    if len(new_col_names) > len(schema_items):
        error.errorType("LS_402")
        return

    new_schema = {}

    for (col, dtype), new_name in zip(schema_items, new_col_names):
        if new_name == "_":
            new_schema[col] = dtype
        else:
            new_schema[new_name] = dtype

    if len(new_col_names) < len(schema_items):
        for col, dtype in schema_items[len(new_col_names):]:
            new_schema[col] = dtype

    new_data = []

    for row in data:
        new_row = {}

        for (col, dtype), new_name in zip(schema_items, new_col_names):
            if new_name == "_":
                new_row[col] = row[col]
            else:
                new_row[new_name] = row[col]

        if len(new_col_names) < len(schema_items):
            for col, dtype in schema_items[len(new_col_names):]:
                new_row[col] = row[col]

        new_data.append(new_row)

    # PERSIST
    table["schema"] = new_schema
    table["data"] = new_data

    with open(tar_dir, "w") as f:
        json.dump(table, f, indent=4)

    print("COLUMN RENAMING COMPLETED.")


def update_columnvalues(cmd):

    # VALIDATE
    if "where" not in [c.lower() for c in cmd]:
        error.errorType("LS_000")
        return

    # PARSE
    tb_name = cmd[1][1:].strip()
    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    wh_index = [i for i, c in enumerate(cmd) if c.lower() == "where"][0]

    raw_values = " ".join(cmd[3:wh_index]).strip()
    where_cmd = cmd[wh_index:]

    if not raw_values:
        error.errorType("LS_103")
        return

    # PARSE 
    rows_raw = []
    current = ""
    inside = False

    for char in raw_values:
        if char == "(":
            inside = True
            current = ""
        elif char == ")":
            inside = False
            rows_raw.append(current.strip())
        elif inside:
            current += char

    if not rows_raw:
        error.errorType("LS_004")
        return

    vals = [v.strip().strip("'").strip('"') for v in rows_raw[0].split(",")]

    if not os.path.exists(tar_dir):
        error.errorType("LS_302")
        return

    # LOAD
    with open(tar_dir, "r") as f:
        table = json.load(f)

    schema = table["schema"]
    data = table["data"]
    schema_items = list(schema.items())
             
    while len(vals) < len(schema_items):
        vals.append("_")

    if len(vals) > len(schema_items):
        error.errorType("LS_402")
        return

    rows_to_update = where.where_cmd(where_cmd, schema, data)

    if not rows_to_update:
        error.errorType("LS_307")
        return

    # UPDATE
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
                        return

                elif dtype == "string":
                    row[col] = str(val)

                elif dtype == "null":
                    if val.lower() != "null":
                        error.errorType("LS_401")
                        return
                    row[col] = None

                else:
                    error.errorType("LS_401")
                    return

            except:
                error.errorType("LS_401")
                return

    # PERSIST
    with open(tar_dir, "w") as f:
        json.dump(table, f, indent=4)

    print(f"UPDATED {len(rows_to_update)} ROW(S) IN '{tb_name}'.")



def update_main(cmd):

    #VALIDATE
    if not state.check_state():
        return

    if state.curr_db is None:
        error.errorType("LS_200")
        return

    if cmd[1][0] != "-" and cmd[2].lower() != "values":
        if len(cmd) != 3:
            error.errorType("LS_000")
            return
        update_db(cmd)
        return

    if cmd[1][0] != "-" and cmd[2].lower() == "values":
        if len(cmd) < 4:
            error.errorType("LS_103")
            return
        update_tablename(cmd)
        return

    if cmd[1][0] != "-":
        error.errorType("LS_007")
        return

    if cmd[2].lower() != "values":
        update_columnname(cmd)
        return

    if cmd[2].lower() == "values":
        update_columnvalues(cmd)
        return

    error.errorType("LS_000")
