from . import error


def parse_where(cmd):

    if len(cmd) != 4:
        error.errorType("LS_000PR")
        return None, None, None

    try:
        return cmd[1], cmd[2], cmd[3]
    except:
        error.errorType("LS_402PR")
        return None, None, None


def resolve_column(col, schema):

    for k in schema:
        if k.lower() == col.lower():
            return k

    error.errorType("LS_305RS")
    return None


def coerce_value(dtype, raw):

    try:
        match dtype:
            case "int":
                return int(raw)

            case "float":
                return float(raw)

            case "bool":
                low = raw.lower()
                if low in ("true", "1"):
                    return True
                if low in ("false", "0"):
                    return False
                error.errorType("LS_401RS")
                return None

            case "string":
                return str(raw)

            case "null":
                if raw.lower() != "null":
                    error.errorType("LS_401RS")
                    return None
                return None

            case _:
                error.errorType("LS_401RS")
                return None
    except:
        error.errorType("LS_401RS")
        return None


def where_cmd(command, schema, rows):

    col, op, raw_val = parse_where(command)
    if col is None:
        return []

    valid_ops = [">", "<", ">=", "=>", "=<", "<=", "==", "!="]

    if op not in valid_ops:
        error.errorType("LS_008VD")
        return []

    real_col = resolve_column(col, schema)
    if real_col is None:
        return []

    dtype = schema[real_col]

    condition = coerce_value(dtype, raw_val)
    if condition is None and dtype != "null":
        return []

    matched = []

    for i, row in enumerate(rows):

        cell = row[real_col]

        match op:
            case ">":
                if cell > condition:
                    matched.append(i)
            case "<":
                if cell < condition:
                    matched.append(i)
            case ">=" | "=>":
                if cell >= condition:
                    matched.append(i)
            case "<=" | "=<":
                if cell <= condition:
                    matched.append(i)
            case "==":
                if cell == condition:
                    matched.append(i)
            case "!=":
                if cell != condition:
                    matched.append(i)

    return matched
