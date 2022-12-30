from sqlite3 import DatabaseError
from timeit import default_timer
from importer import Importer
importer = Importer()


def build_candle(timestamp, pair):
    start = default_timer()
    high_low_vol = importer.db.get('max(price), min(price), sum(volume)', 'trades_' + pair,
                                   f'time >= {str(timestamp)} AND time < {str(timestamp + 3600)}')
    open = importer.db.get(f'first_value(price) OVER (ORDER BY time ) Open', f'trades_{pair}',
                           f'time >= {str(timestamp)} AND time < {str(timestamp + 3600)} LIMIT 1')
    close = importer.db.get(f'first_value(price) OVER (ORDER BY time DESC) Close', f'trades_{pair}',
                            f'time >= {str(timestamp)} AND time < {str(timestamp + 3600)} LIMIT 1')
    candle = (timestamp, open[0], high_low_vol[0], high_low_vol[1], close[0], high_low_vol[2])
    end = default_timer()
    print(f'{pair}, {timestamp}: {candle}', end="")
    print(f', elapsed time: {end - start}')
    return candle


def candles_for_pair(pair):
    last = importer.db.get('value', 'variables', f"name = 'last_{pair}'")[0] / 1000000000
    try:
        first = int(importer.db.get('value', 'variables', f"name = 'last_candle_{pair}'")[0])
    except IndexError as ie:
        print(f"There is no 'last_candle_{pair}' variable in the 'variables' table (Error: {ie}). Putting "
              f"'last_candle_{pair}' variable in to 'variables' table with default value ({importer.starting_since}). "
              f"and using this value as a starting point for candle creation."
              )
        first = importer.starting_since
        importer.db.insert('variables', 'last_candle_' + pair, importer.starting_since)
    importer.db.create_table('candles_' + pair, 'time integer UNIQUE , open , high, low, close, volume')
    start = default_timer()
    timestamp = first
    print(f'{pair} - last = {last}')
    candle_counter = 1
    while timestamp < last:
        print(f'{candle_counter} building candle for: ', end="")
        candle = build_candle(timestamp, pair)
        try:
            importer.db.insert('candles_' + pair, *candle)
        except DatabaseError as de:
            print(f'Candle with timestamp: {timestamp} is already in database. It will be updated. (Error: {de})')
            target = f'high = {candle[2]}, ' \
                     f'low = {candle[3]}, ' \
                     f'close = {candle[4]}, ' \
                     f'volume = {candle[5]}'
            importer.db.update('candles_' + pair, target, f'time = {first}')
            print(f'    Updated candle for: {pair}, {timestamp}, elapsed time: {default_timer() - start}')
        importer.db.update('variables', f'value = {timestamp}', f"name = 'last_candle_{pair}'")
        timestamp += 3600
        candle_counter += 1
    end = default_timer()
    print(f'{pair} - elapsed time for whole pair: {end - start}')
    print('---------------------------')
