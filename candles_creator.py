from timeit import default_timer
from database import Database
from os import getenv
from dotenv import load_dotenv
load_dotenv()


class Creator:
    def __init__(self):
        self.db = Database(getenv('database_name'))
        self.pairs = list(getenv('pairs').split(","))
        self.starting_timestamp = int(getenv('starting_since'))

    def candle(self, timestamp, pair):
        start = default_timer()
        print(f'{pair}, {str(timestamp)}: ' +
              str(list(self.db.get2('sum(volume) as Volume, min(price) as Low, max(price) as High', 'trades_' + pair,
                                    f'time >= {str(timestamp)} AND time < {str(timestamp + 3600)}'))), end="")
        end = default_timer()
        print(f', elapsed time: {end - start}')

    def candles_for_pair(self, pair, first, last):
        start = default_timer()
        timestamp = first
        print(f'{pair} - last = {last}')
        candle = 1
        while timestamp < last:
            print(f'{candle} , ', end="")
            self.candle(timestamp, pair)
            timestamp += 3600
            candle += 1
        end = default_timer()
        print(f'{pair} - elapsed time for whole pair: {end - start}')
        print('---------------------------')
