from bs4 import BeautifulSoup
import requests

class ParallelParliament():
    def __init__(self):
        self.url = 'https://www.parallelparliament.co.uk/mp/'

    def get_mp(self, mp):
        """
        This function obtains the HTML 'soup' for any particular MP.
        """
        self.mp = mp
        self.url = 'https://www.parallelparliament.co.uk/mp/{mp}'.format(mp=self.mp)
        self.r = requests.get(self.url)
        self.soup = BeautifulSoup(self.r.content, 'html.parser')


    def get_mp_twitter(self, mp):
        """
        This function refreshes the HTML soup for a given MP and then returns the twitter pofile. 
        """


        self.get_mp(mp)

        # Get twitter profile
        try:
            self.twitter = self.soup.find(attrs={'class': 'col-sm-12 text-center mt-0'}).contents[7]['href']
        except:
            self.twitter = 'Not Found'
        return self.twitter
