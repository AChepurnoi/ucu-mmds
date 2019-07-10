import tqdm
import pandas as pd
from lxml import etree
import mwapi
from ores_utils import fetch_wp10_score

def strip_tag_name(t):
    t = t.tag
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t

class PageParser(object):
    def __init__(self, lang="en", ores=True, csv=None):
        if csv is not None:
            self.csv = csv
            self.df = pd.read_csv(self.csv)
        else:
            self.isPage = False
            self.pages = []
            self.current_page = {}
        self.lang = lang
        self.ores = ores
        # self.ores_weights = {'Stub': 1, 'Start': 2, 'C': 3, 'B': 4, 'GA': 5, 'FA': 6}
        self.session = mwapi.Session("https://{}.wikipedia.org".format(self.lang))
    
    def handle(self, event, element):
        stipped_elem = strip_tag_name(element)
        if event == "start" and stipped_elem == "page":
            self.isPage = True
        elif event == "end" and stipped_elem == "page":
            self.isPage = False
            self.finalize_page()
        
        if event == "end" and self.isPage:
            self.parse_page_element(element)
    
    def parse_page_element(self, element):
        stripped_tag = strip_tag_name(element)
        if element.text:
            texts = [element.text.strip()] + [child.tail.strip() \
                        for child in element if child.tail]
            self.current_page[stripped_tag] = " ".join(texts)

    def finalize_page(self):
        self.pages.append(self.current_page)
        self.current_page = {}

    def finalize(self):
        self.df = pd.DataFrame(self.pages)
        if self.ores:
            self.fetch_ores()

    def fetch_ores(self, chunks=50):
        print("Fetching ORES")
        final = pd.DataFrame()
        for i in tqdm.tqdm(range(0, len(self.df), chunks)):
            response_doc = next(self.session.get(action="query", prop="revisions",
                                                titles=self.df["title"].iloc[i:i+chunks],
                                                formatversion=2, continuation=True))
            response_doc = pd.DataFrame(response_doc["query"]["pages"])[["title", "revisions"]]
            revision_fn = lambda x: str(x[0]["revid"])
            response_doc["revid"] = response_doc.revisions.apply(revision_fn)
            scores_df = fetch_wp10_score(response_doc["revid"], self.lang)
            final = pd.concat([final, response_doc[["title","revid"]].join(scores_df, on="revid").set_index("title")])
        self.df = self.df.join(final, on="title")


if __name__ == "__main__":
    XML_FILE = "xml_data/sample.xml"
    CSV_OUTPUT = "csv_data/sample-output.csv"

    CSV_READY = True
    CSV_OUTPUT_WITHOUT_ORES = "csv_data/sample-output-0.csv"
    
    if CSV_READY:
        page_parser = PageParser(lang="en", csv=CSV_OUTPUT_WITHOUT_ORES)
        page_parser.fetch_ores()
        page_parser.df.to_csv(CSV_OUTPUT)

    else:
        page_parser = PageParser(lang="en")

        for i, (event, elem) in tqdm.tqdm(enumerate(etree.iterparse(XML_FILE, \
                                                        events=('start', 'end')))):
            page_parser.handle(event, elem)

        page_parser.finalize(ores=True)
        page_parser.df.to_csv(CSV_OUTPUT)