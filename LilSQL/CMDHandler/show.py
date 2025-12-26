import os
import json
import state
from . import error
from . import where


def load_table(tb_name):

    tar_dir = os.path.join(state.curr_dir, f"{tb_name}.json")

    if not os.path.exists(tar_dir):
        error.errorType("LS_302")
        return None, None

    with open(tar_dir, "r") as f:
        table = json.load(f)

    return table["schema"], table["data"]


def apply_where(extra, schema, data):

    for i, token in enumerate(extra):
        if token.lower() == "where":

            command = extra[i:]
            rows = where.where_cmd(command, schema, data)

            if rows is None:
                error.errorType("LS_600")
                return None, None

            return [data[r] for r in rows], extra[:i]

    return data, extra


def resolve_columns(extra, schema):

    if not extra:
        return list(schema.keys())

    if not extra[0].startswith("("):
        error.errorType("LS_000RS")
        return None

    col_raw = " ".join(extra)

    if "(" not in col_raw or ")" not in col_raw:
        error.errorType("LS_000RS")
        return None

    inside = col_raw[col_raw.find("(") + 1 : col_raw.find(")")].strip()

    if inside == "":
        error.errorType("LS_103RS")
        return None

    selected = []
    for c in inside.split(","):
        c = c.strip()
        if not c:
            continue

        found = False
        for key in schema:
            if key.lower() == c.lower():
                selected.append(key)
                found = True
                break

        if not found:
            error.errorType("LS_305RS")
            return None

    return selected


def print_table(data, columns):

    col_widths = {}
    for col in columns:
        longest = max((len(str(r[col])) for r in data), default=0)
        col_widths[col] = max(len(col), longest) + 2

    header = "| "
    for col in columns:
        header += col.ljust(col_widths[col]) + " | "
    print(header)
    print("-" * len(header))

    for row in data:
        line = "| "
        for col in columns:
            line += str(row[col]).ljust(col_widths[col]) + " | "
        print(line)


def show_table(cmd):

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

    schema, data = load_table(tb_name)
    if schema is None:
        return

    extra = cmd[2:]

    data, extra = apply_where(extra, schema, data)
    if data is None:
        return

    cols = resolve_columns(extra, schema)
    if cols is None:
        return

    print_table(data, cols)


def show_main(cmd):

    if not state.check_state():
        return

    if state.curr_db is None:
        error.errorType("LS_200")
        return

    show_table(cmd)
