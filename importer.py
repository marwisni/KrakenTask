from timeit import default_timer
from communicator import Communicator
from database import Database
from os import getenv
from dotenv import load_dotenv
load_dotenv()


class Importer:

    def __init__(self):
        self.communicator = Communicator(getenv('uri'))
        self.db = Database(getenv('database_name'))
        self.pairs = list(getenv('pairs').split(","))

    def request_import(self, result, pair):
        duplicates = 0
        for row in result:
            try:
                self.db.insert('trades_' + pair, None, *row)
            except:
                print('Duplicate - kraken_trade_id: ' + str(row[6]))
                duplicates += 1
        return duplicates

    def pair_import(self, pair):
        flag = True
        iteration = 1
        while flag:
            print(f'This is {iteration} iteration for pair {pair}.')
            try:
                last = str(list(self.db.get('variables', 'value', 'name', 'last_'+pair))[0][0])
            except:
                last = getenv('starting_since')
                self.db.create_table('variables', 'name string UNIQUE, value string')
                self.db.insert('variables', 'last_'+pair, getenv('starting_since'))

            response = self.communicator.trade_request_builder(pair, last).json()
            if iteration == 1:
                key = list(response['result'].keys())[0]
            print(f"Key: {key}")
            result = response['result'][key]
            if len(result) < 1000:
                flag = False
            print(f"Number of rows: {len(result)}")
            print('last: ' + str(last))
            self.db.create_table('trades_'+pair, getenv('trade_columns').replace('\n', ''))

            start = default_timer()
            duplicates = self.request_import(result, pair)
            self.db.update('variables', 'value', response['result']['last'], 'name', 'last_'+pair)
            end = default_timer()
            print('Last iteration:')
            print(f'Imported rows: {len(result) - duplicates}')
            print(f'Duplicates: {duplicates}')
            print(f'Elapsed time: {end - start}')
            iteration += 1
