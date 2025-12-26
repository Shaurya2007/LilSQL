import os
import state
from . import error


def resolve_database(db_name, db_root):

    for db in os.listdir(db_root):
        if db.lower() == db_name.lower():
            return db

    error.errorType("LS_300")
    return None


def use_database(db_name):

    # PARSE
    base_dir = state.root_dir
    db_root = os.path.join(base_dir, "Database")

    # VALIDATE
    real_db = resolve_database(db_name, db_root)
    if real_db is None:
        return

    # EXECUTE + PERSIST
    state.set_db(real_db, os.path.join(db_root, real_db))
    print(f"USING DATABASE '{real_db}'")


def use_main(cmd):

    # VALIDATE
    if not state.check_state():
        return

    if len(cmd) < 2 or cmd[1] == "":
        error.errorType("LS_100")
        return

    # EXECUTE
    use_database(cmd[1])
