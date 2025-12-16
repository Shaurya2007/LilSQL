import os
import CMDHandler.error as error

curr_db = None
curr_dir = None

def check_state():

    # VALIDATE
    if curr_db is None and curr_dir is not None:
        error.errorType("LS_600")
        return False

    if curr_db is not None:
        if curr_dir is None or not os.path.exists(curr_dir):
            error.errorType("LS_600")
            return False

    # EXECUTE
    return True
