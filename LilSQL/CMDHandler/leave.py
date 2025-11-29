import state
import os

def leave_database():
    if state.curr_db is None:
        print("NO DATABASE SELECTED.")
        return

    print(f"LEFT DATABASE '{state.curr_db}'.")
    state.curr_db = None
    state.curr_dir = None

def leave_main(cmd):
    if len(cmd) != 1:
        print("INVALID LEAVE COMMAND SYNTAX.")
        return
    
    leave_database()