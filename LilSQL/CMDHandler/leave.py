import state
from . import error


def leave_database():

    if state.curr_db is None:
        error.errorType("LS_200")
        return

    print(f"LEFT DATABASE '{state.curr_db}'.")
    state.clear_db()


def leave_main(cmd):

    if not state.check_state():
        return

    leave_database()
