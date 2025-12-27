from router import route
import state
import os
import json
import logs

def local_init():
    target_root = os.getenv("LOCALAPPDATA")
    root_dir = os.path.join(target_root, "LilSQL")

    if not os.path.exists(root_dir):
        os.makedirs(root_dir, exist_ok=True)

    logs.logs_init()

def read_input():
    return input(f"LILSQL({state.curr_db if state.curr_db else ''})=> ")


def should_exit(inp):
    return inp.strip().lower() in ("exit", "quit")


def repl_loop():

    while True:
        inp = read_input()

        if should_exit(inp):
            print("Exiting MiniSQL CLI.")
            break

        route(inp)


def main():
    local_init()
    print("Welcome to MiniSQL CLI. Type 'exit' or 'quit' to leave.")
    repl_loop()


if __name__ == "__main__":
    main()
