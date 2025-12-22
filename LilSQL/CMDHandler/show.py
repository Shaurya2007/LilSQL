import os
import json
import state
from . import error
from . import where


def show_table(cmd):

    # VALIDATE

    if len(cmd) < 2:
        error.errorType("LS_100")
        return

    if not cmd[1].startswith("-"):
        error.errorType("LS_000")
        return

    tb_name = cmd[1][1:].strip()

    if tb_name == "":
        error.errorType("LS_002")
        return

    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tar_dir):
        error.errorType("LS_302")
        return

    # LOAD

    with open(tar_dir, "r") as f:
        table = json.load(f)

    schema = table["schema"]
    data = table["data"]

    extra = cmd[2:]
    col_filter_raw = None

    # WHERE

    if "where" in extra:
        where_index = extra.index("where")
        command = extra[where_index:]

        filtered_rows = where.where_cmd(command, schema, data)
        
        if filtered_rows is None:
            error.errorType("LS_600")
            return
        
        data = [data[i] for i in filtered_rows]

        extra = extra[:where_index]

    # COLUMN FILTER 

    if extra:
        if not extra[0].startswith("("):
            error.errorType("LS_000")
            return

        col_filter_raw = " ".join(extra)

        if "(" not in col_filter_raw or ")" not in col_filter_raw:
            error.errorType("LS_000")
            return

        inside = col_filter_raw[
            col_filter_raw.find("(") + 1 : col_filter_raw.find(")")
        ].strip()

        if inside == "":
            error.errorType("LS_103")
            return

        selected_cols = [c.strip() for c in inside.split(",") if c.strip()]

        for col in selected_cols:
            if col not in schema:
                error.errorType("LS_305")
                return
    else:
        selected_cols = list(schema.keys())

    # PRINT

    col_widths = {}
    for col in selected_cols:
        longest = max((len(str(r[col])) for r in data), default=0)
        col_widths[col] = max(len(col), longest) + 2

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

    if not state.check_state():
        return

    if state.curr_db is None:
        error.errorType("LS_200")
        return

    show_table(cmd)
