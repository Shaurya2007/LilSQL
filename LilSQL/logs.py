# Example log entry structure:
#
# {
#   "log_id": 184,
#   "timestamp": "2025-01-03T22:41:09",
#   "command": "update -users values (_,Alice) where id == 1",

#   "db": "testDB",
#   "table": "users",

#   "phase": "EXECUTE",
#   "status": "SUCCESS",

#   "action": "UPDATE",

#   "before": [
#     { "id": 1, "name": "Bob", "age": 22 }
#   ],

#   "after": [
#     { "id": 1, "name": "Alice", "age": 22 }
#   ],

#   "where": "id == 1"
# }

import os
import json
import datetime

logs_dir = os.path.join(os.getenv("LOCALAPPDATA"),"LilSQL", "Logs")

def logs_init():

    os.makedirs(logs_dir, exist_ok=True)

    user_log = os.path.join(logs_dir, "user.logs")
    meta_file = os.path.join(logs_dir, "engine.meta")

    if not os.path.exists(user_log):
        open(user_log, "a").close()

    if not os.path.exists(meta_file):
        meta = {"prev_log_id": 0}
        with open(meta_file, "w") as f:
            json.dump(meta, f)

def log_entryid():

    meta_file = os.path.join(logs_dir, "engine.meta")
    with open(meta_file, "r") as f:
        meta = json.load(f)

    log_id = meta["prev_log_id"] + 1
    meta["prev_log_id"] = log_id

    with open(meta_file, "w") as f:
        json.dump(meta, f)

    return log_id

def log_entrytime():

    return datetime.datetime.now().isoformat(timespec="seconds")

def log_entrymain(entry: dict):

    log_entry = {
        **entry,
        "log_id": log_entryid(),
        "timestamp": log_entrytime()
    }

    user_log = os.path.join(logs_dir, "user.logs")
    with open(user_log, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
        f.flush()