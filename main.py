import candles_creator

print('Importing data from Kraken API has been started...')
for pair in candles_creator.importer.pairs:
    candles_creator.importer.pair_import(pair)
print('Importing data from Kraken API has been finished.')
print('Creation of candles has been started...')
for pair in candles_creator.importer.pairs:
    candles_creator.candles_for_pair(pair)
print('Creation of candles has been finished.')
