import os
import json
import state



def show_table(cmd):

    if len(cmd) < 2:
        print("INCOMPLETE COMMAND.")
        return

    if not cmd[1].startswith("-"):
        print("INVALID COMMAND FORMAT.")
        return

    tb_name = cmd[1][1:].strip()

    if tb_name == "":
        print("INVALID TABLE NAME.")
        return

    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tar_dir):
        print("TABLE DOES NOT EXIST.")
        return

    with open(tar_dir, 'r') as f:
        table = json.load(f)

    schema = table["schema"]
    data = table["data"]
    col_filter_raw = None
    extra = cmd[2:]

    if extra:
        if not extra[0].startswith("("):
            print("INVALID COMMAND FORMAT.")
            return

    for token in cmd[2:]:
        if "(" in token:
            index = cmd.index(token)
            col_filter_raw = " ".join(cmd[index:])
            break

    if col_filter_raw is None:
        selected_cols = list(schema.keys())

    else:
        if "(" not in col_filter_raw or ")" not in col_filter_raw:
            print("INVALID COLUMN FILTER FORMAT.")
            return

        inside = col_filter_raw[col_filter_raw.find("(")+1 : col_filter_raw.find(")")].strip()

        if inside == "":
            print("EMPTY COLUMN LIST.")
            return

        selected_cols = [c.strip() for c in inside.split(",") if c.strip()]

        for col in selected_cols:
            if col not in schema:
                print(f"COLUMN '{col}' DOES NOT EXIST.")
                return

    col_widths = {}
    for col in selected_cols:
        longest_data = max((len(str(r[col])) for r in data), default=0)
        col_widths[col] = max(len(col), longest_data) + 2

    header = "| "
    for col in selected_cols:
        header += col.ljust(col_widths[col]) + " | "
    print(header)

    print("-" * len(header))

    for row in data:
        line = "| "
        for col in selected_cols:
            line += str(row[col]).ljust(col_widths[col]) + " | "
        print(line)

    



def show_main(cmd):

    if state.curr_db is None:
        print("NO DATABASE SELECTED.")
        return

    if len(cmd) < 2:
        print("INCOMPLETE COMMAND.")
        return

    show_table(cmd)
