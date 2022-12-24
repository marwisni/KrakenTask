from timeit import default_timer
from communicator import Communicator
from database import Database
from os import getenv
from dotenv import load_dotenv
load_dotenv()

communicator = Communicator(getenv('uri'))
db = Database(getenv('database_name'))
pairs = list(getenv('pairs').split(","))

flag = True
iteration = 0
while flag:
    print(f'This is {iteration} iteration.')
    try:
        last = str(list(db.get('variables', 'value', 'name', 'last'))[0][0])
    except:
        last = getenv('starting_since')
        db.create_table('variables', 'name string UNIQUE, value string')
        db.insert('variables', 'last', getenv('starting_since'))

    response = communicator.trade_request_builder(pairs[0], last).json()
    result = response['result']['XXBTZEUR']
    if len(result) < 1000:
        flag = False
    print(f"Number of rows: {len(result)}")
    print('last: ' + last)
    db.create_table('trades', getenv('trade_columns').replace('\n', ''))

    start = default_timer()
    for row in result:
        try:
            db.insert('trades', None, *row)
        except:
            print('Error: ' + str(row[6]))
    db.update('variables', 'value', response['result']['last'], 'name', 'last')
    end = default_timer()
    print(f'Last iteration elapsed time is: {end - start}')
    iteration += 1
