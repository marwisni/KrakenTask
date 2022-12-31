from sqlite3 import DatabaseError
from timeit import default_timer
from communicator import Communicator
from database import Database
from os import getenv
from dotenv import load_dotenv

load_dotenv()


def get_pair_key(response):
    """
    Returns pair's key for api response (it is different that you provide in request)
    :param response: dict
        Response from the api.
    :return: str
        Key of particular in the response dictionary, f.e. 'XETHZUSD' for ETHUSD pair.
    """
    key = list(response['result'].keys())[0]
    print(f"key='{key}'")
    return key


class Importer:
    """
    A class to get data from Kraken api and save it in the database.

    Attributes
    ----------
    communicator : Communicator
        Object for communicate with api.
    db : Database
        Object to manage project's database.
    pairs : list [str]
        List of pairs that will be imported from the Kraken api.
    starting_since: int
        Timestamp which will be used to start importing process.
    trade_columns: list [str]
        List of columns' names with additional attributes which will be provided during table creation in the database.
        F. e. 'UNIQUE', 'AUTOINCREMENT', etc.
    """
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
        """
        Puts single request's result in the database and returns number of trades that already was in database (
        duplicates)
        :param result: dict,
            dictionary from Kraken api response.
        :param pair: str,
            currency pair for which request has been made and for which result will be put in the database.
        :return: integer,
            number of trades (rows) that was tried to put in the database but already was there (duplicates).
        """
        duplicates = 0
        for row in result:
            try:
                self.db.insert('trades_' + pair, None, *row)
            except DatabaseError as de:
                print(f'Duplicate - kraken_trade_id: {row[6]}, {de}')
                duplicates += 1
        return duplicates

    def __get_last_imported_trade_time_ns(self, pair):
        """Returns timestamp in nanoseconds of last inserted trade in the database for particular currency pair. """
        return self.db.get('value', 'variables', f"name = 'last_{pair}'")[0]

    def get_last_imported_trade_time_s(self, pair):
        """Returns timestamp in seconds of last inserted trade in the database for particular currency pair. """
        return self.__get_last_imported_trade_time_ns(pair) / 1000000000

    def __set_start_point_for_import(self, pair):
        """
        Gets and returns start point for import for particular currency pair from database or if it is not present yet
        uses default value instead.
        """
        try:
            return self.__get_last_imported_trade_time_ns(pair)
        except IndexError as ie:
            print(f"There is no 'last_{pair}' variable in the 'variables' table (Error: {ie}). Creating table "
                  f"'variables' and using default value ({self.starting_since}) as starting point for import.")
            return self.starting_since

    def __prepare_import(self, pair, *columns):
        """
        Creates table for trades if not exists, creates index of time columns in this trades table and returns starting
        timestamp for import process. Creates table 'variables' if not exists.
        :param pair: str,
            currency pair for which import will be done, f.e. "XBTEUR'.
        :param columns: list [str],
            list of columns' names with additional attributes which will be created in the database, f. e.
            'kraken_trade_id integer UNIQUE'.
        :return: integer,
            starting timestamp for import process.
        """
        self.db.create_table('trades_' + pair, ', '.join(columns))
        self.db.create_table('variables', 'name string UNIQUE, value string')
        self.db.create_index('Time_' + pair, 'trades_' + pair, 'time ASC')
        return self.__set_start_point_for_import(pair)

    def pair_import(self, pair):
        """
        Imports all available trades for particular currency pair from Kraken api since last import
        (or from default date if it is first import) and saves it in the database. Puts timestamp of
        last imported timestamp to 'variables' table.
        :param pair: str
            pair for which data will be imported
        :return:
            None
        """
        flag = True
        iteration = 1
        last = self.__prepare_import(pair, *self.trade_columns)
        while flag:
            print(f'This is {iteration} iteration for pair {pair}:')
            response = self.communicator.trade_request_builder(pair, last).json()

            if iteration == 1:
                key = get_pair_key(response)

            result = response['result'][key]

            if len(result) < 1000:
                flag = False

            start = default_timer()
            print(f'number of rows to import: {len(result)}, timestamp of last trade in Kraken api response: {last}')

            duplicates = self.put_one_request_result_to_db(result, pair)

            last = response['result']['last']
            self.db.replace('variables', 'name, value', f"'last_{pair}', {last}")

            print(f'Last iteration - imported rows: {len(result) - duplicates}, duplicates: {duplicates}, '
                  f'elapsed time for whole pair import: {default_timer() - start}')

            iteration += 1
