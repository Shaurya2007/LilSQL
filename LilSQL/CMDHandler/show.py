import os
import json
import state
import error


def show_table(cmd):

    # VALIDATE
    if len(cmd) < 2:
        error.errorType("LS_100")
        return

    if not cmd[1].startswith("-"):
        error.errorType("LS_000")
        return

    # PARSE
    tb_name = cmd[1][1:].strip()

    # VALIDATE
    if tb_name == "":
        error.errorType("LS_002")
        return

    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tar_dir):
        error.errorType("LS_302")
        return

    # EXECUTE
    with open(tar_dir, 'r') as f:
        table = json.load(f)

    schema = table["schema"]
    data = table["data"]

    col_filter_raw = None
    extra = cmd[2:]

    # VALIDATE
    if extra:
        if not extra[0].startswith("("):
            error.errorType("LS_000")
            return

    # PARSE
    for token in cmd[2:]:
        if "(" in token:
            index = cmd.index(token)
            col_filter_raw = " ".join(cmd[index:])
            break

    # VALIDATE
    if col_filter_raw is None:
        selected_cols = list(schema.keys())

    else:
        if "(" not in col_filter_raw or ")" not in col_filter_raw:
            error.errorType("LS_000")
            return

        inside = col_filter_raw[col_filter_raw.find("(")+1 : col_filter_raw.find(")")].strip()

        if inside == "":
            error.errorType("LS_103")
            return

        selected_cols = [c.strip() for c in inside.split(",") if c.strip()]

        for col in selected_cols:
            if col not in schema:
                error.errorType("LS_305")
                return

    # EXECUTE
    col_widths = {}
    for col in selected_cols:
        longest_data = max((len(str(r[col])) for r in data), default=0)
        col_widths[col] = max(len(col), longest_data) + 2

    # PERSIST
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

    # VALIDATE
    if not state.check_state():
        return

    if state.curr_db is None:
        error.errorType("LS_200")
        return

    # EXECUTE + PERSIST
    show_table(cmd)
