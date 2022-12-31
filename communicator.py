import requests


class Communicator:
    """
    A class to communicate with Kraken API.
    ...
    Attributes
    ----------
    data_type : str
        type of data we want to get, f.e. Trades, OHLC, Depth
    version : str
        version of Kraken API
    api_type : str
        type of Kraken API, can be 'public' or 'private'
    uri : str
        Kraken API uri address

    Methods
    -------
    trade_request_builder(pair, since):
        Builds the request for 'Trades' endpoint and returns response
    """
    def __init__(self, uri):
        """
        Constructs communicator object with given uri param.
        param uri: str
            Kraken API uri address
        """
        self.data_type = None
        self.version = None
        self.api_type = None
        self.uri = uri

    def trade_request_builder(self, pair, since):
        """
        Builds the request for 'Trades' endpoint and returns response
        :param pair: str
            Currency pair, f.e. 'XBTEUR', 'ETHUSD'
        :param since: str
            Unix timestamp in seconds or nanoseconds as start point for import
        :return: dict
            Api response as dictionary.
        """
        self.data_type = "Trades"
        self.version = '0'
        self.api_type = 'public'
        return requests.get(f'{self.uri}/{self.version}/{self.api_type}/{self.data_type}?pair={pair}&since={since}')
