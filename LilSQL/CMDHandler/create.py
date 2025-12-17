import os
import json
import state
from . import error


def create_database(db_name):

    # PARSE
    invalid_name = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']

    # VALIDATE
    if state.curr_db is not None:
        error.errorType("LS_201")
        return
    
    if db_name.strip() == "" or db_name[0] == "-" or any(char in db_name for char in invalid_name):
        error.errorType("LS_001")           
        return

    # EXECUTE
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    if not os.path.exists(os.path.join(base_dir, "Database")):
        os.makedirs(os.path.join(base_dir, "Database"))

    db_path = os.path.join(base_dir, "Database", db_name)

    if os.path.exists(db_path):
        error.errorType("LS_301")
        return
    
    # PERSIST
    os.makedirs(db_path)
    print(f"DATABASE '{db_name}' CREATED.")


def create_table(cmd):

    # PARSE
    tb_name = cmd[1][1:].strip()
    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    raw_schema_values = " ".join(cmd[2:])
    ref_schema_values = [v.strip() for v in raw_schema_values.split(",")]

    schema_values = {}
    valid_types = {"int", "float", "bool", "string", "null"}
    invalid_name = ['/','\\','\\',':','*','?','"','<','>','|']

    new_cols = []

    # VALIDATE
    for v in ref_schema_values:
        if ":" not in v:
            error.errorType("LS_005")
            return

        col, dtype = v.split(":", 1)
        col = col.strip()
        dtype = dtype.strip()

        if dtype not in valid_types:
            error.errorType("LS_006")
            return

        new_cols.append((col, dtype))

    # EXECUTE
    if not os.path.exists(tar_dir):

        if any(char in tb_name for char in invalid_name):
            error.errorType("LS_002")
            return

        for col, dtype in new_cols:
            schema_values[col] = dtype

        new_table = {
            "schema": schema_values,
            "data": []
        }

        # PERSIST
        with open(tar_dir, "w") as f:
            json.dump(new_table, f, indent=4)

        print(f"TABLE '{tb_name}' CREATED.")
        return

    else:
        with open(tar_dir, "r") as f:
            table = json.load(f)

        schema = table["schema"]
        data = table["data"]

        # VALIDATE
        for col, dtype in new_cols:

            col = col.strip()

            if any(char in col for char in invalid_name):
                error.errorType("LS_003")
                return
            
            if col in schema:
                error.errorType("LS_306")
                return

            # EXECUTE 
            schema[col] = dtype  

            for row in data:
                if dtype == valid_types[0]:
                    row[col] = 0
                elif dtype == valid_types[1]:
                    row[col] = 0.0
                elif dtype == valid_types[2]:
                    row[col] = False
                elif dtype == valid_types[3]:
                    row[col] = ""
                elif dtype == valid_types[4]:
                    row[col] = None

        # PERSIST
        with open(tar_dir, "w") as f:
            json.dump(table, f, indent=4)

        print(f"ADDED {len(new_cols)} NEW COLUMN(S) TO TABLE '{tb_name}'.")
        return


def create_columnvalues(cmd):

    # PARSE
    tb_name = cmd[1][1:].strip()
    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")
    valid_types = ["int", "float", "bool", "string", "null"]
    raw_values = " ".join(cmd[3:]).strip()

    rows_raw = []
    current = ""
    inside = False

    # VALIDATE
    if cmd[2].lower() != "values" or not raw_values:
        error.errorType("LS_100")
        return

    if not os.path.exists(tar_dir):
        error.errorType("LS_302")
        return

    # PARSE
    for char in raw_values:
        if char == '(':
            inside = True
            current = ""
        elif char == ')':
            inside = False
            rows_raw.append(current.strip())
        elif inside:
            current += char

    parsed_rows = []
    for raw in rows_raw:
        vals = [v.strip().strip("'").strip('"') for v in raw.split(",")]
        parsed_rows.append(vals)

    # EXECUTE
    with open(tar_dir, 'r') as f:
        table = json.load(f)

    schema = table["schema"]
    data = table["data"]
    schema_items = list(schema.items())
    col_count = len(schema_items)

    inserted = 0
    for vals in parsed_rows:

        while len(vals) < col_count:
            vals.append("")

        if len(vals) > col_count:
            error.errorType("LS_402")
            return

        new_row = {}
        for (col, dtype), val in zip(schema_items, vals):

            if val == "_" or val == "":
                new_row[col] = None
                continue

            try:
                if dtype == valid_types[0]:
                    new_row[col] = int(val)
                elif dtype == valid_types[1]:
                    new_row[col] = float(val)
                elif dtype == valid_types[2]:
                    lower = val.lower()
                    if lower in ("true", "1"):
                        new_row[col] = True
                        continue
                    elif lower in ("false", "0"):
                        new_row[col] = False
                        continue
                    
                    error.errorType("LS_006")
                    return
                elif dtype == "string":
                    if val is None:
                        new_row[col] = None
                        continue

                    new_row[col] = str(val)

                elif dtype == valid_types[4]:
                    if val.lower() != "null":
                        error.errorType("LS_006")
                        return
                    new_row[col] = None
                                
            except Exception as e:  
                print(type(e).__name__, e)
                return
            
        data.append(new_row)
        inserted += 1

    # PERSIST
    with open(tar_dir, "w") as f:
        json.dump(table, f, indent=4)

    print(f"INSERTED {inserted} ROW(S) INTO '{tb_name}'.")


def create_main(cmd):

    # VALIDATE
    if not state.check_state():
        return

    try:
        # PARSE
        name = cmd[1]

        # VALIDATE
        if name[0] != "-":
            if len(cmd) != 2:
                error.errorType("LS_000")
                return
            
            # EXECUTE + PERSIST
            create_database(name)
            return            

        if state.curr_db is None:
            error.errorType("LS_200")
            return

        # PARSE
        if len(cmd) > 2 and cmd[2].lower() == "values":
            create_columnvalues(cmd)
            return

        create_table(cmd)
        return

    except IndexError:
        error.errorType("LS_100")
