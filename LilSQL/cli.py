import os
from router import route
import state

def repl_start():

    # INPUT LOOP
    while True:
        inp = input(f"LILSQL({state.curr_db if state.curr_db else ''})=> ").lower()

        # VALIDATE
        if inp == "exit" or inp == "quit":
            print("Exiting MiniSQL CLI.")
            break

        # ROUTER
        route(inp)
            

def main():

    # START
    print("Welcome to MiniSQL CLI. Type 'exit' or 'quit' to leave.")
    repl_start()


if __name__ == "__main__":
    main()