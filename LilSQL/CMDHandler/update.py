import os
import json
import state



def update_db(cmd):

    old_path = state.curr_dir
    new_path = os.path.join(os.path.dirname(state.curr_dir), cmd[2])

    if state.curr_db is None or state.curr_dir is None:
        print("NO DATABASE SELECTED.")
        return

    if len(cmd) < 3 or not cmd[2]:
        print("INCOMPLETE COMMAND.")
        return
    
    if os.path.exists(old_path):
        os.rename(old_path, new_path)
        print(f"DATABASE RENAMED TO '{cmd[2]}'.")

        state.curr_db = cmd[2]
        state.curr_dir = new_path

    else:
        print("DATABASE DOES NOT EXIST OR DATABASE NOT CONNECTED.")



def update_tablename(cmd):

    old_tb_names = [table[:-5] for table in os.listdir(state.curr_dir) if table.endswith(".json")]
    curr_name = " ".join(cmd[3:]).strip()
    new_tb_names = [v.strip() for v in curr_name.split(",")]
    invalid_name = ['/','\\',':','*','?','"','<','>','|']

    if state.curr_db is None:
        print("NO DATABASE SELECTED.")
        return

    if len(old_tb_names) < len(new_tb_names):
        print("MISMATCH IN TABLE COUNT.")
        return

    for nm in new_tb_names:
        if nm == "_":
            continue
        if any(ch in nm for ch in invalid_name) or nm == "":
            print(f"INVALID TABLE NAME '{nm}'.")
            return

    for i in range(len(new_tb_names)):
        new_name = new_tb_names[i]
        old_name = old_tb_names[i]

        if new_name == "_":
            continue 

        old_path = os.path.join(state.curr_dir, f"{old_name}.json")
        new_path = os.path.join(state.curr_dir, f"{new_name}.json")

        if not os.path.exists(old_path):
            print("TABLE DOES NOT EXIST.")
            return

        if os.path.exists(new_path):
            print(f"TABLE '{new_name}' ALREADY EXISTS.")
            return

        os.rename(old_path, new_path)
        print(f"TABLE RENAMED TO '{new_name}'.")

    print("TABLE RENAMING COMPLETED.")





def update_columnname(cmd):

    raw_values = " ".join(cmd[2:])
    new_col_names = [v.strip() for v in raw_values.split(",")]
    invalid_chars = set(['/','\\',':','*','?','"',"'",'<','>','|'])
    cleaned = []
    seen = set()

    if "(" in raw_values or ")" in raw_values:
        print("INVALID COLUMN NAME PARENTHESES.")
        return
    
    for nm in new_col_names:
        nm = nm.strip()

        if nm == "_":
            cleaned.append("_")
            continue

        if nm == "":
            print("INVALID COLUMN NAME ''.")
            return

        if any(ch in nm for ch in invalid_chars):
            print(f"INVALID COLUMN NAME '{nm}'.")
            return

        if nm in seen:
            print(f"DUPLICATE COLUMN NAME '{nm}'.")
            return

        seen.add(nm)
        cleaned.append(nm)

    new_col_names = cleaned

    tb_name = cmd[1][1:].strip()
    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tar_dir):
        print("TABLE DOES NOT EXIST.")
        return

    with open(tar_dir, 'r') as f:
        table = json.load(f)

    schema = table["schema"]
    data = table["data"]
    schema_items = list(schema.items()) 
    new_col_count = len(new_col_names)
    old_col_count = len(schema_items)

    if new_col_count > old_col_count:
        print("MISMATCH IN COLUMN COUNT.")
        return
    
    new_schema = {}

    for (col, dtype), new_name in zip(schema_items, new_col_names):

        if new_name == "_":  
            new_schema[col] = dtype
        else:
            new_schema[new_name] = dtype

    if new_col_count < old_col_count:
        for col, dtype in schema_items[new_col_count:]:
            new_schema[col] = dtype

    new_data = []

    for row in data:

        new_row = {}

        for (col, dtype), new_name in zip(schema_items, new_col_names):
            if new_name == "_":
                new_row[col] = row[col]
            else:
                new_row[new_name] = row[col]

        if new_col_count < old_col_count:
            for col, dtype in schema_items[new_col_count:]:
                new_row[col] = row[col]

        new_data.append(new_row)

    table["schema"] = new_schema
    table["data"] = new_data

    with open(tar_dir, "w") as f:
        json.dump(table, f, indent=4)

    print("COLUMN RENAMING COMPLETED.")




def update_columnvalues(cmd):

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
        if char == "(":
            inside = True
            current = ""
        elif char == ")":
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

    with open(tar_dir, "r") as f:
        table = json.load(f)

    schema = table["schema"]
    data = table["data"]
    schema_items = list(schema.items())

    col_count = len(schema_items)
    existing_rows = len(data)
    incoming_rows = len(parsed_rows)

    if incoming_rows > existing_rows:
        print("ROWS COUNT MISMATCH.")
        return

    for row_index in range(incoming_rows):
        vals = parsed_rows[row_index]
        old_row = data[row_index]

        while len(vals) < col_count:
            vals.append("_")

        if len(vals) > col_count:
            print("VALUE COUNT MISMATCH.")
            return

        for (col, dtype), val in zip(schema_items, vals):

            if val == "_":
                continue 

            try:
                if dtype == "int":
                    old_row[col] = int(val)

                elif dtype == "float":
                    old_row[col] = float(val)

                elif dtype == "bool":
                    lower = val.lower()
                    if lower in ("true", "1"):
                        old_row[col] = True
                    elif lower in ("false", "0"):
                        old_row[col] = False
                    else:
                        print(f"TYPE MISMATCH FOR COLUMN '{col}'. EXPECTED 'bool'.")
                        return

                elif dtype == "string":
                    old_row[col] = str(val)

                elif dtype == "null":
                    if val.lower() == "null":
                        old_row[col] = None
                    else:
                        print(f"TYPE MISMATCH FOR COLUMN '{col}'. EXPECTED 'null'.")
                        return

                else:
                    print(f"UNKNOWN DATA TYPE '{dtype}' FOR COLUMN '{col}'.")
                    return

            except:
                print(f"TYPE MISMATCH FOR COLUMN '{col}'.")
                return

    with open(tar_dir, "w") as f:
        json.dump(table, f, indent=4)

    print(f"UPDATED {incoming_rows} ROW(S) IN '{tb_name}'.")





def update_main(cmd):

            
    if state.curr_db is None:
        print("NO DATABASE SELECTED.")
        return
    
    if len(cmd) < 3:
        print("INCOMPLETE UPDATE COMMAND.")
        return

    if cmd[1][0] != "-" and cmd[2].lower() != "values":
        if len(cmd) != 3:
            print("INVALID DATABASE RENAME SYNTAX.")
            return
        
        update_db(cmd)
        return

    if cmd[1][0] != "-" and cmd[2].lower() == "values":
        if len(cmd) < 4:
            print("NO TABLE NAMES PROVIDED.")
            return
        update_tablename(cmd)
        return

    if cmd[1][0] != "-":
        print("INVALID UPDATE TARGET.")
        return

    if cmd[2].lower() != "values":
        update_columnname(cmd)
        return

    if cmd[2].lower() == "values":
        update_columnvalues(cmd)
        return

    print("INVALID UPDATE COMMAND FORMAT.")
