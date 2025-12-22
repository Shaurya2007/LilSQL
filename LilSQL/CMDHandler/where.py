from . import error

def where_cmd(command, schema, rows):

    cmd = command

    if len(cmd) != 4:
        error.errorType("LS_000")
        return []

    try: 
        c_in = cmd[1]
        op = cmd[2]
        raw_val = cmd[3]

    except Exception as e:
        error.errorType("LS_402")
        return
    
    filtered_rows = []
    valid_filters = [">", "<", ">=", "=>", "=<", "<=", "==", "!="]

    # VALIDATE

    try:

        if op not in valid_filters:
            error.errorType("LS_008")
            return []

        if c_in not in schema:
            error.errorType("LS_305")
            return []
    
    except:
        error.errorType("LS_000")
        return []

    dtype = schema[c_in]

    # TYPE COERCION

    try:
        match dtype:
            case "int":
                condition = int(raw_val)

            case "float":
                condition = float(raw_val)

            case "bool":
                low = raw_val.lower()
                if low in ("true", "1"):
                    condition = True
                elif low in ("false", "0"):
                    condition = False
                else:
                    error.errorType("LS_401")

            case "string":
                condition = str(raw_val)

            case "null":
                if raw_val.lower() != "null":
                    error.errorType("LS_401")
                condition = None

            case _:
                error.errorType("LS_401")
                return []
    except:
        error.errorType("LS_401")
        return []

    # FILTER

    for row in range(len(rows)):
        cell = rows[row][c_in]

        match op:
            case ">":
                if cell > condition:
                    filtered_rows.append(row)
            case "<":
                if cell < condition:
                    filtered_rows.append(row)
            case ">=":
                if cell >= condition:
                    filtered_rows.append(row)
            case "=>":
                if cell >= condition:
                    filtered_rows.append(row)
            case "<=":
                if cell <= condition:
                    filtered_rows.append(row)
            case "=<":
                if cell <= condition:
                    filtered_rows.append(row)
            case "==":
                if cell == condition:
                    filtered_rows.append(row)
            case "!=":
                if cell != condition:
                    filtered_rows.append(row)

    return filtered_rows
