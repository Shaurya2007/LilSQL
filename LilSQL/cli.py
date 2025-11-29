#CLI interface for LilSQL
import os
from router import route
import state

def repl_start():
    
    while True:
        inp = input(f"LILSQL({state.curr_db if state.curr_db else ''})=> ").lower()

        if inp == "exit" or inp == "quit":
            print("Exiting MiniSQL CLI.")
            break
        else:
            route(inp)
            

def main():
    
    print("Welcome to MiniSQL CLI. Type 'exit' or 'quit' to leave.")
    repl_start()

if __name__ == "__main__":
    main()