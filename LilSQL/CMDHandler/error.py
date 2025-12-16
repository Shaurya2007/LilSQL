#Error LS_0XX
def invalid_cmd(code,num):

    match num:

        case "00":
            print(f"ERROR {code}: INVALID COMMAND.")
        case "01":
            print(f"ERROR {code}: INVALID DATABASE NAME.")
        case "02":
            print(f"ERROR {code}: INVALID TABLE NAME.")
        case "03":
            print(f"ERROR {code}: INVALID COLUMN NAME.")
        case "04":
            print(f"ERROR {code}: INVALID VALUE FORMAT.")
        case "05":
            print(f"ERROR {code}: INVALID SCHEMA FORMAT.")
        case "06":
            print(f"ERROR {code}: INVALID DATA TYPE.")
        case "07":
            print(f"ERROR {code}: INVALID TARGET.")

#Error LS_1XX
def incomplete_cmd(code,num):

    match num:

        case "00":
            print(f"ERROR {code}: INCOMPLETE COMMAND.")
        case "01":
            print(f"ERROR {code}: INCOMPLETE SCHEMA DEFINITION.")
        case "02":
            print(f"ERROR {code}: INCOMPLETE DATA TYPE DEFINITION.")
        case "03":
            print(f"ERROR {code}: INCOMPLETE/EMPTY VALUE GROUP.")

#Error LS_2XX
def state_cmd(code,num):
    
    match num:
    
        case "00":
            print(f"ERROR {code}: NOT CONNECTED TO A DATABASE.")
        case "01":
            print(f"ERROR {code}: ALREADY CONNECTED TO A DATABASE.")

#Error LS_3XX
def exist_cmd(code,num):
    match num:

        case "00":
            print(f"ERROR {code}: DATABASE DOES NOT EXIST.")
        case "01":
            print(f"ERROR {code}: DATABASE ALREADY EXISTS.")
        case "02":
            print(f"ERROR {code}: TABLE DOES NOT EXIST.")
        case "03":
            print(f"ERROR {code}: TABLE ALREADY EXISTS.")
        case "04":
            print(f"ERROR {code}: DUPLICATE COLUMN NAME.")
        case "05":
            print(f"ERROR {code}: COLUMN DOES NOT EXIST.")
        case "06":
            print(f"ERROR {code}: COLUMN ALREADY EXISTS.")

#Error LS_4XX
def mismatch_cmd(code,num):

    match num:

        case "00":
            print(f"ERROR {code}: SCHEMA MISMATCH.")
        case "01":
            print(f"ERROR {code}: DATA TYPE MISMATCH.")
        case "02":
            print(f"ERROR {code}: COUNT MISMATCH.")

#Error LS_5XX
def delete_cmd(code,num):

    match num:

        case "00":
            print(f"ERROR {code}: FAILED TO DELETE DATABASE.")
        case "01":
            print(f"ERROR {code}: FAILED TO DELETE TABLE.")
        case "02":
            print(f"ERROR {code}: FAILED TO DELETE COLUMNS.")
        case "03":
            print(f"ERROR {code}: FAILED TO DELETE ROWS.")

#Error LS_6XX
def unexpected_cmd(code,num):

    match num:

        case "00":
            print(f"ERROR {code}: UNEXPECTED STATE ERROR OCCURRED.")

def errorType(ercode):

    code = ercode[3:]
    ercategory = code[0]
    ernumber = code[1:]

        
    match ercategory:

        case "0":
            invalid_cmd(ercode,ernumber)

        case "1":
            incomplete_cmd(ercode,ernumber)

        case "2":
            state_cmd(ercode,ernumber)

        case "3":
            exist_cmd(ercode,ernumber)

        case "4":
            mismatch_cmd(ercode,ernumber)

        case "5":
            delete_cmd(ercode,ernumber)

        case "6":
            unexpected_cmd(ercode,ernumber)