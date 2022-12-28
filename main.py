from importer import Importer
from candles_creator import Creator

importer = Importer()
creator = Creator()

print('Importing data from kraken API has been started...')
for pair in importer.pairs:
    importer.pair_import(pair)
print('Importing data from kraken API has been finished.')
print('Creation of candles has been started...')
for pair in importer.pairs:
    creator.candles_for_pair(pair)
print('Creation of candles has been finished.')
