from . import error
import os
import json
import shutil
import state


logs_dir = os.path.join(os.getenv("LOCALAPPDATA"),"LilSQL", "Logs")
user_logs = os.path.join(logs_dir, "user.logs")
meta_file = os.path.join(logs_dir, "engine.meta")


def parse_undo(cmd):
    if len(cmd) == 1:
        return 1

    if len(cmd) == 2 and cmd[1].isdigit():
        return int(cmd[1])

    error.errorType("LS_007PR")  
    return None


def load_final_logs(n):
    logs = []

    with open(meta_file, "r") as f:
        meta = json.load(f)
        cursor = meta["undo_cursor"]

    with open(user_logs, "r") as f:
        for line in f:
            try:
                entry = json.loads(line)

            except json.JSONDecodeError:
                continue

            if (entry.get("status") == "SUCCESS" and entry.get("log_id") <= cursor):
                logs.append(entry)

    if n > len(logs):
        error.errorType("LS_702UN")
        return None

    return logs[-n:][::-1]



def undo_create_database(entry):
    db_name = entry["db"]

    db_path = os.path.join(state.root_dir,"Database",db_name)

    if os.path.exists(db_path):
        shutil.rmtree(db_path)


def undo_create_table(entry,tb_path):

    if os.path.exists(tb_path):
        os.remove(tb_path)


def undo_alter_table_add_columns(entry,tb_path):

    with open(tb_path, "r") as f:
        table = json.load(f)

    if not isinstance(table, dict) or "schema" not in table or "data" not in table:
        return

    old_schema = entry["before"]
    new_schema = entry["after"]
    added_cols = [col for col in new_schema if col not in old_schema]

    table["schema"] = old_schema

    for row in table["data"]:
        for col in added_cols:
            if col in row:
                del row[col]

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)


def undo_added_rows(entry,tb_path):

    with open(tb_path, "r") as f:
        table = json.load(f)

    inserted_rows = entry["after"]

    table["data"] = [row for row in table["data"] if row not in inserted_rows]

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)


def undo_delete_database(entry):
    db_name = entry["before"]

    db_path = os.path.join(state.root_dir,"Database",db_name)

    if not os.path.exists(db_path):
        os.makedirs(db_path)


def undo_delete_table(entry,tb_path):
    
    with open(tb_path, "w") as f:
        json.dump(entry["before"], f, indent=4)


def undo_delete_columns(entry,tb_path):

    with open(tb_path, "r") as f:
        table = json.load(f)

    if not isinstance(table, dict) or "schema" not in table or "data" not in table:
        return

    old_schema = entry["before"]
    new_schema = entry["after"]
    deleted_cols = {col: dtype for col, dtype in old_schema.items() if col not in new_schema}

    table["schema"] = old_schema

    for row in table["data"]:
        for col, dtype in deleted_cols.items():
            if dtype == "int":
                row[col] = 0
            elif dtype == "float":
                row[col] = 0.0
            elif dtype == "bool":
                row[col] = False
            elif dtype == "string":
                row[col] = ""
            elif dtype == "null":
                row[col] = None

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)


def undo_delete_rows_where(entry,tb_path):

    with open(tb_path, "r") as f:
        table = json.load(f)

    if not isinstance(table, dict) or "data" not in table:
        return

    rows = entry["before"]
    indexes = entry["row_indexes"]

    for index, row in sorted(zip(indexes, rows)):
        table["data"].insert(index, row)

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)


def undo_delete_all_rows(entry,tb_path):

    with open(tb_path, "r") as f:
        table = json.load(f)

    if not isinstance(table, dict) or "data" not in table:
        return

    table["data"] = entry["before"]

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)


def undo_delete_row_literal(entry,tb_path):

    with open(tb_path, "r") as f:
        table = json.load(f)

    if not isinstance(table, dict) or "data" not in table:
        return

    row = entry["before"][0]
    index = entry["row_indexes"][0]

    table["data"].insert(index, row)

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)


def undo_update_database_name(entry):
    old_name = entry["before"]
    new_name = entry["after"]

    new_path = os.path.join(state.root_dir,"Database",new_name)
    old_path = os.path.join(state.root_dir,"Database",old_name)

    if os.path.exists(new_path):
        os.rename(new_path, old_path)


def undo_update_table_name(entry):
    db = entry["db"]
    old_tables = entry["before"]
    new_tables = entry["after"]

    db_path = os.path.join(state.root_dir,"Database",db)

    for old, new in zip(old_tables, new_tables):
        new_path = os.path.join(db_path, f"{new}.json")
        old_path = os.path.join(db_path, f"{old}.json")

        if os.path.exists(new_path):
            os.rename(new_path, old_path)


def undo_update_column_name(entry,tb_path):

    with open(tb_path, "r") as f:
        table = json.load(f)

    old_schema = entry["before"]
    new_schema = entry["after"]
    data = table["data"]

    reverse_map = {}
    for old_col, old_type in old_schema.items():
        for new_col, new_type in new_schema.items():
            if old_type == new_type:
                reverse_map[new_col] = old_col
                break

    restored_data = []
    for row in data:
        restored_row = {}
        for new_col, value in row.items():
            old_col = reverse_map.get(new_col)
            if old_col:
                restored_row[old_col] = value
        restored_data.append(restored_row)

    table["schema"] = old_schema
    table["data"] = restored_data

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)


def undo_update_rows(entry,tb_path):

    with open(tb_path, "r") as f:
        table = json.load(f)

    before_rows = entry["before"]
    after_rows = entry["after"]

    for i, row in enumerate(table["data"]):
        if row in after_rows:
            idx = after_rows.index(row)
            table["data"][i] = before_rows[idx]

    with open(tb_path, "w") as f:
        json.dump(table, f, indent=4)



def apply_inverse(entry):
    tb_path = os.path.join(state.root_dir,"Database",entry["db"],f"{entry['table']}.json")
    action = entry["action"]

    match action:
        case "CREATE_DATABASE":
            undo_create_database(entry)

        case "CREATE_TABLE":
            undo_create_table(entry,tb_path)

        case "ALTER_TABLE_ADD_COLUMNS":
            undo_alter_table_add_columns(entry,tb_path)
        
        case "INSERT":
            undo_added_rows(entry,tb_path)


        case "DELETE_DATABASE":
            undo_delete_database(entry)

        case "DELETE_TABLE":
            undo_delete_table(entry,tb_path)

        case "DELETE_COLUMNS":
            undo_delete_columns(entry,tb_path)

        case "DELETE_ROWS_WHERE":
            undo_delete_rows_where(entry,tb_path)

        case "DELETE_ALL_ROWS":
            undo_delete_all_rows(entry,tb_path)
        
        case "DELETE_ROW_LITERAL":
            undo_delete_row_literal(entry,tb_path)

        case "UPDATE_DATABASE_NAME":
            undo_update_database_name(entry)

        case "UPDATE_TABLE_NAME":
            undo_update_table_name(entry)

        case "UPDATE_COLUMN_NAME":
            undo_update_column_name(entry,tb_path)

        case "UPDATE_ROWS":
            undo_update_rows(entry,tb_path)


        case _:
            error.errorType("LS_600UN")


def undo_main(cmd):
    steps = parse_undo(cmd)
    if steps is None:
        return

    entries = load_final_logs(steps)
    if entries is None:
        return

    for entry in entries:
        apply_inverse(entry)

    with open(meta_file, "r") as f:
        meta = json.load(f)

    meta["undo_cursor"] = 0

    with open(meta_file, "w") as f:
        json.dump(meta, f, indent=4)

    print(f"UNDO COMPLETED ({steps} STEP{'S' if steps > 1 else ''}).")



