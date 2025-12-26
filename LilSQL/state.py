import os
import CMDHandler.error as error

target_root = os.getenv("LOCALAPPDATA")
root_dir = os.path.join(target_root, "LilSQL")

curr_db = None
curr_dir = None


def has_db():
    return curr_db is not None


def has_dir():
    return curr_dir is not None


def get_db():
    return curr_db


def get_dir():
    return curr_dir


def set_db(db_name, db_dir):
    global curr_db, curr_dir

    curr_db = db_name
    curr_dir = db_dir


def clear_db():
    global curr_db, curr_dir

    curr_db = None
    curr_dir = None


def check_state():

    if curr_db is None and curr_dir is not None:
        error.errorType("LS_600VD")
        return False

    if curr_db is not None:
        if curr_dir is None or not os.path.exists(curr_dir):
            error.errorType("LS_600")
            return False

    return True
