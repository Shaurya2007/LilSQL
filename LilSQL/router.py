from CMDHandler.create import create_main
from CMDHandler.delete import delete_main
from CMDHandler.use import use_main
from CMDHandler.update import update_main
from CMDHandler.show import show_main
from CMDHandler.leave import leave_main
from CMDHandler.error import errorType
from CMDHandler.undo import undo_main


def len_check(cmd, elen):
    return len(cmd) >= elen


commands = {
    "create": (create_main, 2),
    "use": (use_main, 2),
    "delete": (delete_main, 2),
    "update": (update_main, 2),
    "show": (show_main, 2),
    "leave": (leave_main, 1),
    "undo": (undo_main, 1)
}


def route(inp):

    # PARSE
    cmd = inp.split()

    # VALIDATE
    if not cmd:
        return

    keyword = cmd[0].lower() 

    if keyword not in commands:
        errorType("LS_000VD")
        return

    handler, min_len = commands[keyword]
    if not len_check(cmd, min_len):
        errorType("LS_100VD")
        return

    # EXECUTE
    handler(cmd)
