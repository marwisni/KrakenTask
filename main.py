from importer import Importer
from candles_creator import Creator

importer = Importer()
creator = Creator()

for pair in importer.pairs:
    importer.pair_import(pair)

for pair in importer.pairs:
    last = float(list(importer.db.get('variables', 'value', 'name', 'last_'+ pair))[0][0])/1000000000
    creator.candles_for_pair(pair, creator.starting_timestamp, last)
