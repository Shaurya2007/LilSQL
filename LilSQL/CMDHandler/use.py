import state
import os

def use_database(db_name):

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_root = os.path.join(base_dir, "Database")

    if db_name not in os.listdir(db_root):
        print("DATABASE DOES NOT EXIST.")
        return
    
    state.curr_db = db_name
    state.curr_dir = os.path.join(base_dir, "Database", db_name)
    print(f"USING DATABASE '{db_name}'")

def use_main(cmd):
    if cmd[1] != "":
        use_database(cmd[1])


#How will i disconnect from a database?