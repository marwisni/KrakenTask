from sqlite3 import DatabaseError
from timeit import default_timer
from communicator import Communicator
from database import Database
from os import getenv
from dotenv import load_dotenv

load_dotenv()


class Importer:
    communicator = Communicator(getenv('uri'))
    db = Database(getenv('database_name'))
    pairs = list(getenv('pairs').split(","))
    starting_since = int(getenv('starting_since'))
    trade_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT',
                     'price float',
                     'volume float',
                     'time float',
                     'buy_sell char',
                     'market_limit char',
                     'miscellaneous string',
                     'kraken_trade_id integer UNIQUE']

    def put_one_request_result_to_db(self, result, pair):
        duplicates = 0
        for row in result:
            try:
                self.db.insert('trades_' + pair, None, *row)
            except DatabaseError as de:
                print(f'Duplicate - kraken_trade_id: {row[6]}, {de}')
                duplicates += 1
        return duplicates

    def __prepare_table(self, pair, *columns):
        self.db.create_table('trades_' + pair, ', '.join(columns))
        self.db.create_index('Time_' + pair, 'trades_' + pair, 'time ASC')

    def __get_last_imported_row(self, pair):
        try:
            return self.db.get('value', 'variables', f"name = 'last_{pair}'")[0]
        except IndexError as ie:
            print(f"There is no 'last_{pair}' variable in the 'variables' table (Error: {ie}). Creating table "
                  f"'variables' and/or putting there 'last_{pair}' variable with default value ({self.starting_since}) "
                  f"and using this value as a starting point for import.")
            self.db.create_table('variables', 'name string UNIQUE, value string')
            self.db.insert('variables', 'last_' + pair, self.starting_since)
            return self.starting_since

    def pair_import(self, pair):
        flag = True
        iteration = 1
        if iteration == 1:
            self.__prepare_table(pair, *self.trade_columns)
        while flag:
            print(f'This is {iteration} iteration for pair {pair}.')
            last = self.__get_last_imported_row(pair)
            response = self.communicator.trade_request_builder(pair, last).json()
            if iteration == 1:
                key = list(response['result'].keys())[0]
                print(f"Key: {key}")
            result = response['result'][key]
            if len(result) < 1000:
                flag = False
            print(f"Number of rows to import: {len(result)}")
            print('last: ' + str(last))
            start = default_timer()
            duplicates = self.put_one_request_result_to_db(result, pair)
            self.db.update('variables', f"value = {response['result']['last']}", f"name = 'last_{pair}'")
            end = default_timer()
            print('Last iteration:')
            print(f'Imported rows: {len(result) - duplicates}')
            print(f'Duplicates: {duplicates}')
            print(f'Elapsed time: {end - start}')
            iteration += 1
