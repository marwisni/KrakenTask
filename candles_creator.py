from sqlite3 import DatabaseError
from timeit import default_timer
from importer import Importer

importer = Importer()


def build_candle(timestamp, pair):
    """
    Creates OHLCV candle for particular timestamp and currency pair.
    :param timestamp: int,
        unix timestamp for which OHLCV candle will be created.
    :param pair: str,
        currency pair for which OHLCV candle will be created.
    :return:
        tuple (timestamp, open, high, low, close, volume)
    """
    start = default_timer()
    high_low_vol = importer.db.get('max(price), min(price), sum(volume)', 'trades_' + pair,
                                   f'time >= {timestamp} AND time < {timestamp + 3600}')
    open = importer.db.get(f'first_value(price) OVER (ORDER BY time ) Open', f'trades_{pair}',
                           f'time >= {timestamp} AND time < {timestamp + 3600} LIMIT 1')
    close = importer.db.get(f'first_value(price) OVER (ORDER BY time DESC) Close', f'trades_{pair}',
                            f'time >= {timestamp} AND time < {timestamp + 3600} LIMIT 1')
    candle = (timestamp, open[0], high_low_vol[0], high_low_vol[1], close[0], high_low_vol[2])
    end = default_timer()
    print(f'{pair}, {timestamp}: {candle}', end="")
    print(f', elapsed time: {end - start}')
    return candle


def prepare_candles_creation(pair):
    """
    Gets starting point for candles creation from database for particular currency pair or if it is not present
    uses default value instead and returns it. Creates 'candles_{pair}' table in the database if it doesn't exist.

    :param pair: str,
        currency pair for which starting point for candles creation will be returned and eventually table
        'candles_{pair}' will be created.
    :return:
        integer - timestamp starting point for candles creation for particular currency pair
    """
    try:
        return int(importer.db.get('value', 'variables', f"name = 'last_candle_{pair}'")[0])
    except IndexError as ie:
        print(f"There is no 'last_candle_{pair}' variable in the 'variables' table (Error: {ie}). Creating table "
              f"'candles_{pair}' if it doesnt exists and using default value ({importer.starting_since}) "
              f"as a starting point for candle creation.")
        return importer.starting_since
    finally:
        importer.db.create_table(f'candles_{pair}', 'time integer UNIQUE , open , high, low, close, volume')


def candles_for_pair(pair):
    """
    Creates OHLCV candles for particular currency pair for all trades from database since last import
        (or from default date if it is first import) and saves it in the database. Puts timestamp of
        last created candle timestamp to 'variables' table.
    :param pair:
    :return:
        None
    """
    last = importer.get_last_imported_trade_time_s(pair)
    timestamp = prepare_candles_creation(pair)
    start = default_timer()
    print(f'{pair}: starting timestamp = {timestamp},  timestamp of last trade in the database = {last}')
    candles_counter = 1
    while timestamp < last:
        print(f'Building candle no. {candles_counter} for: ', end="")
        candle = build_candle(timestamp, pair)
        try:
            importer.db.insert('candles_' + pair, *candle)
        except DatabaseError as de:
            print(f'Candle with timestamp: {timestamp} is already in database. It will be updated. (Error: {de})')
            target = f'high = {candle[2]}, ' \
                     f'low = {candle[3]}, ' \
                     f'close = {candle[4]}, ' \
                     f'volume = {candle[5]}'
            importer.db.update('candles_' + pair, target, f'time = {timestamp}')
            print(f'    Updated candle for: {pair}, {timestamp}, elapsed time: {default_timer() - start}')
        importer.db.replace('variables', 'name, value', f"'last_candle_{pair}', {timestamp}")
        timestamp += 3600
        candles_counter += 1
    print(f'{pair} - elapsed time for whole pair: {default_timer() - start}')
    print('---------------------------')
