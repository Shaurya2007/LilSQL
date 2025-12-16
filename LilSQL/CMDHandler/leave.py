import state
import os
import error


def leave_database():

    # VALIDATE
    if state.curr_db is None:
        error.errorType("LS_200")
        return

    # EXECUTE + PERSIST
    print(f"LEFT DATABASE '{state.curr_db}'.")
    state.curr_db = None
    state.curr_dir = None


def leave_main(cmd):

    # VALIDATE
    if not state.check_state():
        return

    # EXECUTE + PERSIST
    leave_database()
