#Strictly for routing commands to their respective handlers
from CMDHandler.create import create_main
from CMDHandler.delete import delete_main
from CMDHandler.use import use_main
from CMDHandler.update import update_main
from CMDHandler.show import show_main
from CMDHandler.leave import leave_main

def route(inp):

    cmd = list(inp.split())

    if cmd[0] == "create":
        create_main(cmd)
    
    elif cmd[0] == "use":
        use_main(cmd)

    elif cmd[0] == "delete":
        delete_main(cmd)

    elif cmd[0] == "update":
        update_main(cmd)
    
    elif cmd[0] == "show":
        show_main(cmd)

    elif cmd[0] == "leave":
        leave_main(cmd)