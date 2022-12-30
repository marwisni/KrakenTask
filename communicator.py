import requests


class Communicator:

    def __init__(self, uri):
        self.data_type = None
        self.version = None
        self.api_type = None
        self.uri = uri

    def trade_request_builder(self, pair, since):
        self.version = '0'
        self.api_type = 'public'
        self.data_type = "Trades"
        response = requests.get(f'{self.uri}/{self.version}/{self.api_type}/{self.data_type}?pair={pair}&since={since}')
        return response

