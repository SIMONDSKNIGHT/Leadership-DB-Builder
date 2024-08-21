import requests
from bs4 import BeautifulSoup

class BaseScraper:
    def __init__(self, base_url, headers=None):
        self.base_url = base_url
        self.headers = headers

    def get_html(self, url, method='GET', data=None):
        """Fetches HTML content from a given URL using the specified HTTP method."""
        try:
            if method == 'POST':
                response = requests.post(url, headers=self.headers, data=data)
            else:
                response = requests.get(url, headers=self.headers)
            response.raise_for_status()  # Raise HTTPError for bad responses
            return response.text
        except requests.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    def parse_html(self, html):
        #Parses HTML content using BeautifulSoup.
        return BeautifulSoup(html, 'html.parser')