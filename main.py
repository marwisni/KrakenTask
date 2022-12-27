from timeit import default_timer
from communicator import Communicator
from database import Database
from os import getenv
from dotenv import load_dotenv
load_dotenv()

communicator = Communicator(getenv('uri'))
db = Database(getenv('database_name'))
pairs = list(getenv('pairs').split(","))

for pair in pairs:
    flag = True
    iteration = 1
    while flag:
        print(f'This is {iteration} iteration for pair {pair}.')
        try:
            last = str(list(db.get('variables', 'value', 'name', 'last_'+pair))[0][0])
        except:
            last = getenv('starting_since')
            db.create_table('variables', 'name string UNIQUE, value string')
            db.insert('variables', 'last_'+pair, getenv('starting_since'))

        response = communicator.trade_request_builder(pair, last).json()
        if iteration == 1:
            key = list(response['result'].keys())[0]
        print(f"Key: {key}")
        result = response['result'][key]
        if len(result) < 1000:
            flag = False
        print(f"Number of rows: {len(result)}")
        print('last: ' + str(last))
        db.create_table('trades_'+pair, getenv('trade_columns').replace('\n', ''))

        start = default_timer()
        for row in result:
            try:
                db.insert('trades_'+pair, None, *row)
            except:
                print('Error: ' + str(row[6]))
        db.update('variables', 'value', response['result']['last'], 'name', 'last_'+pair)
        end = default_timer()
        print(f'Last iteration elapsed time is: {end - start}')
        iteration += 1
