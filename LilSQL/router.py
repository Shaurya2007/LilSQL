from CMDHandler.create import create_main
from CMDHandler.delete import delete_main
from CMDHandler.use import use_main
from CMDHandler.update import update_main
from CMDHandler.show import show_main
from CMDHandler.leave import leave_main
from CMDHandler.error import errorType

def len_check(cmd, elen):

    # VALIDATE
    if len(cmd) < elen:
        return False
    return True

    
def route(inp):

    # PARSE
    cmd = list(inp.split())

    # VALIDATE
    if not cmd:
        return
    
    if cmd[0] == "create":

        if not len_check(cmd, 2):
            errorType("LS_100")
            return

        # EXECUTE + PERSIST
        create_main(cmd)
    

    elif cmd[0] == "use":

        if not len_check(cmd, 2):
            error.errorType("LS_100")
            return

        # EXECUTE + PERSIST
        use_main(cmd)


    elif cmd[0] == "delete":

        if not len_check(cmd, 2):
            errorType("LS_100")
            return

        # EXECUTE + PERSIST
        delete_main(cmd)


    elif cmd[0] == "update":

        if not len_check(cmd, 2):
            errorType("LS_100")
            return

        # EXECUTE + PERSIST
        update_main(cmd)
    

    elif cmd[0] == "show":

        if not len_check(cmd, 2):
            errorType("LS_100")
            return

        # EXECUTE + PERSIST
        show_main(cmd)


    elif cmd[0] == "leave":

        if not len_check(cmd, 2):
            errorType("LS_100")
            return

        # EXECUTE + PERSIST
        leave_main(cmd)
