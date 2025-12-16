import state
import os
import error

def use_database(db_name):

    # PARSE
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_root = os.path.join(base_dir, "Database")

    # VALIDATE
    if db_name not in os.listdir(db_root):
        error.errorType("LS_300")
        return
    
    # EXECUTE + PERSIST
    state.curr_db = db_name
    state.curr_dir = os.path.join(base_dir, "Database", db_name)
    print(f"USING DATABASE '{db_name}'")


def use_main(cmd):

    # VALIDATE
    if not state.check_state():
        return
    
    # PARSE
    if cmd[1] != "":
        use_database(cmd[1])
