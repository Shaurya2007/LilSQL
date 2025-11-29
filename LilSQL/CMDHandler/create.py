#This will contain functions to create(and connect incase of db's) databases and tables
import os
import json
import state



def create_database(db_name):

    invalid_name = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']

    if db_name.strip() == "":
        print("INVALID DATABASE NAME.")
        return

    if db_name[0] == "-":
        print("INVALID DATABASE NAME.")
        return

    if state.curr_db is not None:
        print("CANNOT CREATE DATABASE WHILE USING ANOTHER DATABASE.")
        return

    if any(char in db_name for char in invalid_name):
        print("INVALID DATABASE NAME.")
        return

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(base_dir, "Database", db_name)

    if not os.path.exists(db_path):
        os.makedirs(db_path)
        print(f"DATABASE '{db_name}' CREATED.")
    else:
        print("DATABASE ALREADY EXISTS.")




def create_table(cmd):

    tb_name = cmd[1][1:].strip()
    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    raw_schema_values = " ".join(cmd[2:])
    ref_schema_values = [v.strip() for v in raw_schema_values.split(",")]

    schema_values = {}
    valid_types = {"int", "float", "string", "bool", "null", "num"}
    invalid_name = ['/','\\','\\',':','*','?','"','<','>','|']

    new_cols = []  

    for v in ref_schema_values:
        if ":" in v:
            col, dtype = v.split(":", 1)
            col = col.strip()
            dtype = dtype.strip()

            if dtype not in valid_types:
                print(f"INVALID DATA TYPE '{dtype}' FOR COLUMN '{col}'.")
                return

            new_cols.append((col, dtype))
        else:
            print("INVALID SCHEMA FORMAT.")
            return

    if not os.path.exists(tar_dir):

        if any(char in tb_name for char in invalid_name):
            print("INVALID TABLE NAME.")
            return

        for col, dtype in new_cols:
            schema_values[col] = dtype

        new_table = {
            "schema": schema_values,
            "data": []
        }

        with open(tar_dir, "w") as f:
            json.dump(new_table, f, indent=4)

        print(f"TABLE '{tb_name}' CREATED.")
        return

    else:
        with open(tar_dir, "r") as f:
            table = json.load(f)

        schema = table["schema"]
        data = table["data"]

        for col, dtype in new_cols:

            col = col.strip()

            if any(char in col for char in invalid_name):
                print("INVALID COLUMN NAME.")
                return
            
            if col in schema:
                print(f"COLUMN '{col}' ALREADY EXISTS.")
                return

            schema[col] = dtype  

            for row in data:
                if dtype == "int":
                    row[col] = 0
                elif dtype == "float":
                    row[col] = 0.0
                elif dtype == "string":
                    row[col] = ""
                elif dtype == "bool":
                    row[col] = False
                elif dtype == "null":
                    row[col] = None
                elif dtype == "num":
                    row[col] = 0

        with open(tar_dir, "w") as f:
            json.dump(table, f, indent=4)

        print(f"ADDED {len(new_cols)} NEW COLUMN(S) TO TABLE '{tb_name}'.")
        return




def create_columnvalues(cmd):

    tb_name = cmd[1][1:].strip()
    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    raw_values = " ".join(cmd[3:]).strip()
    rows_raw = []
    current = ""
    inside = False

    if not raw_values:
        print("EMPTY VALUE GROUP.")
        return

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

    if not os.path.exists(tar_dir):
        print("TABLE DOES NOT EXIST.")
        return

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
            print("VALUE COUNT MISMATCH.")
            return

        new_row = {}
        for (col, dtype), val in zip(schema_items, vals):

            if val == "_" or val == "":
                new_row[col] = None
                continue

            try:
                if dtype == "int":
                    new_row[col] = int(val)

                elif dtype == "float":
                    new_row[col] = float(val)

                elif dtype == "bool":
                    lower = val.lower()
                    if lower in ("true", "1"):
                        new_row[col] = True
                    elif lower in ("false", "0"):
                        new_row[col] = False
                    else:
                        print(f"TYPE MISMATCH FOR COLUMN '{col}'. EXPECTED 'bool'.")
                        return

                elif dtype == "string":
                    new_row[col] = str(val)

                elif dtype == "null":
                    if val.lower() == "null":
                        new_row[col] = None
                    else:
                        print(f"TYPE MISMATCH FOR COLUMN '{col}'. EXPECTED 'null'.")
                        return

                else:
                    print(f"UNKNOWN DATA TYPE '{dtype}' FOR COLUMN '{col}'.")
                    return

            except ValueError:
                print(f"TYPE MISMATCH FOR COLUMN '{col}'.")
                return

        data.append(new_row)
        inserted += 1

    with open(tar_dir, "w") as f:
        json.dump(table, f, indent=4)

    print(f"INSERTED {inserted} ROW(S) INTO '{tb_name}'.")


def create_main(cmd):
    try:
        if len(cmd) < 2:
            print("INCOMPLETE COMMAND.")
            return

        name = cmd[1]

        if name[0] != "-":
            if len(cmd) == 2:
                create_database(name)
                return
            else:
                print("INVALID COMMAND FORMAT.")
                return

        if state.curr_db is None:
            print("NO DATABASE SELECTED.")
            return

        if len(cmd) > 2 and cmd[2].lower() == "values":
            create_columnvalues(cmd)
            return

        create_table(cmd)
        return

    except IndexError:
        print("INCOMPLETE COMMAND.")
