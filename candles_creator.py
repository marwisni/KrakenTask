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

    def build_candle(self, timestamp, pair):
        start = default_timer()
        high_low_vol = self.db.get('max(price), min(price), sum(volume)', 'trades_' + pair,
                                    f'time >= {str(timestamp)} AND time < {str(timestamp + 3600)}')
        open = self.db.get(f'first_value(price) OVER (ORDER BY time ) Open', f'trades_{pair}',
                            f'time >= {str(timestamp)} AND time < {str(timestamp + 3600)} LIMIT 1')
        close = self.db.get(f'first_value(price) OVER (ORDER BY time DESC) Close', f'trades_{pair}',
                             f'time >= {str(timestamp)} AND time < {str(timestamp + 3600)} LIMIT 1')
        candle = (timestamp, open[0], high_low_vol[0], high_low_vol[1], close[0], high_low_vol[2])
        end = default_timer()
        print(f'{pair}, {timestamp}: {candle}', end="")
        print(f', elapsed time: {end - start}')
        return candle

    def candles_for_pair(self, pair):
        last = self.db.get('value', 'variables', f"name = 'last_{pair}'")[0] / 1000000000
        try:
            first = int(self.db.get('value', 'variables', f"name = 'last_candle_{pair}'")[0])
        except:
            first = self.starting_timestamp
            self.db.create_table('variables', 'name string UNIQUE, value string')
            self.db.insert('variables', 'last_candle_' + pair, self.starting_timestamp)
        self.db.create_table('candles_' + pair, 'time integer UNIQUE , open , high, low, close, volume')
        start = default_timer()
        timestamp = first
        print(f'{pair} - last = {last}')
        candle_counter = 1
        while timestamp < last:  # last for full work or 1656680400 for test
            print(f'{candle_counter} building candle for: ', end="")
            candle = self.build_candle(timestamp, pair)
            try:
                self.db.insert('candles_' + pair, *candle)
            except:
                target = f'high = {candle[2]}, ' \
                         f'low = {candle[3]}, ' \
                         f'close = {candle[4]}, ' \
                         f'volume = {candle[5]}'
                self.db.update('candles_' + pair, target, f'time = {first}')
                print(f'    Updated candle for: {pair}, {timestamp}, elapsed time: {default_timer() - start}')
            self.db.update('variables', f'value = {timestamp}', f"name = 'last_candle_{pair}'")
            timestamp += 3600
            candle_counter += 1
        end = default_timer()
        print(f'{pair} - elapsed time for whole pair: {end - start}')
        print('---------------------------')
