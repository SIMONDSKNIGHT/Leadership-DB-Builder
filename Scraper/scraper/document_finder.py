from .base_scraper import BaseScraper

class DocumentFinder(BaseScraper):
    def __init__(self, base_url, headers, search_criteria):
        super().__init__(base_url, headers)
        self.search_criteria = search_criteria

    def find_documents(self):
        """Finds and returns a list of document links and their metadata."""
        html = self.get_html(self.base_url, method='POST', data=self.search_criteria)
        if html:
            soup = self.parse_html(html)
            document_info = []
            for row in soup.find_all('tr', class_='even'):
                title_td = row.find('td', class_='title')
                if title_td:
                    a_tag = title_td.find('a', href=True)
                    if a_tag and a_tag['href'].endswith('.pdf'):
                        document_metadata = {
                            "company_name": row.find('td', class_='companyname').text.strip(),
                            "company_code": row.find('td', class_='code').text.strip(),
                            "file_timestamp": row.find('td', class_='time').text.strip(),
                            "document_name": a_tag.text.strip()
                        }
                        document_info.append((a_tag['href'], document_metadata))
            return document_info
        return []

    def find_related_documents(self, criteria):
        #Finds related documents based on given criteria.
        document_links = self.find_documents()
        # Implement logic to filter related documents based on criteria
        related_documents = [link for link in document_links if criteria in link]
        return related_documents
    def save_html(self, html):
        #saves html
        with open("output.html", "w") as f:
            f.write(html)