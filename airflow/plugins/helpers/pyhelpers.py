import requests


class PyHelpers():
    def __init__(self, url):
        self.url = url

    def get_data_from_api(self):
        data = requests.get(url=self.url)

