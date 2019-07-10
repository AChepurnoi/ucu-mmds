import tqdm
import pandas as pd
from lxml import etree
import resource

import mwapi
import mwtypes
import requests

def strip_tag_name(t):
    t = t.tag
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t

def score2sum(score_doc, weights):
    weighted_sum = 0
    for cl, proba in score_doc['probability'].items():
        weighted_sum += weights[cl] * proba
    return weighted_sum

def fetch_wp10_score(rev_id, lang):
    response = requests.get('https://ores.wikimedia.org/v3/scores/{}wiki/{}/wp10'.format(lang, rev_id))
    try:
        return response.json()['enwiki']['scores'][str(rev_id)]['wp10']['score']
    except:
        return None


class PageParser(object):
    def __init__(self, lang="en", fetch_ores=True):
        self.isPage = False
        self.pages = []
        self.current_page = {}
        self.lang = lang
        self.fetch_ores = fetch_ores
        self.finalize = self.finalize_page_with_ores if self.fetch_ores \
                        else self.finalize_page
        self.ores_weights = {'Stub': 1, 'Start': 2, 'C': 3, 'B': 4, 'GA': 5, 'FA': 6}
        self.session = mwapi.Session("https://{}.wikipedia.org".format(self.lang))
        self.rvlimit = 100
    
    def handle(self, event, element):
        stipped_elem = strip_tag_name(element)
        if event == "start" and stipped_elem == "page":
            self.isPage = True
        elif event == "end" and stipped_elem == "page":
            self.isPage = False
            self.finalize()
        
        if event == "end" and self.isPage:
            self.parse_page_element(element)
    
    def parse_page_element(self, element):
        stripped_tag = strip_tag_name(element)
        if element.text:
            texts = [element.text.strip()] + [child.tail.strip() \
                        for child in element if child.tail]
            self.current_page[stripped_tag] = " ".join(texts)

    def get_revision(self):
        for response_doc in self.session.get(action='query', prop='revisions',
                                             titles=self.current_page["title"],
                                             rvprop=['ids', 'timestamp'],
                                             rvlimit=self.rvlimit, rvdir="older", 
                                             formatversion=2, continuation=True):
            rev_docs = pd.DataFrame(response_doc['query']['pages'][0]['revisions'])
            rev = rev_docs[rev_docs.timestamp == self.current_page["timestamp"]]
            if len(rev) == 1:
                rev_id = rev["revid"].values[0]
                try:
                    return score2sum(fetch_wp10_score(rev_id, self.lang), self.ores_weights)
                except:
                    return None
        return None

    def finalize_page_with_ores(self):
        self.current_page["ORES"] = self.get_revision()
        self.pages.append(self.current_page)
        self.current_page = {}

    def finalize_page(self):
        self.pages.append(self.current_page)
        self.current_page = {}

if __name__ == "__main__":
    XML_FILE = "xml_data/sample.xml"
    CSV_OUTPUT = "csv_data/sample-output.csv"

    page_parser = PageParser(lang="en", fetch_ores=True)

    for i, (event, elem) in tqdm.tqdm(enumerate(etree.iterparse(XML_FILE, \
                                                    events=('start', 'end')))):
        page_parser.handle(event, elem)

    df = pd.DataFrame(page_parser.pages)
    df.to_csv(CSV_OUTPUT)