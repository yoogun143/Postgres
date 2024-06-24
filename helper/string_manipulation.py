import re

def sql_to_list(sql_path):
    # Open and read the file as a single buffer
    fd = open(sql_path, 'r')
    commands = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    commands = re.sub('--(.*)(?=\n)', '', commands)
    commands = commands.replace('\n','')
    commands = ' '.join(commands.split())
    commands = commands.split(';')
    # print(commands[0])

    return commands

def join_stock_string(symbol):
    if isinstance(symbol, (list,tuple)):
        stock_string = ','.join(symbol)
    else: 
        stock_string = symbol

    return stock_string
