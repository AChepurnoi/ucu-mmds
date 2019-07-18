import mwapi
import pandas as pd
import tqdm
from lxml import etree
from ores_utils import fetch_ores_score
from code_timer import CodeTimer
import os


def strip_tag_name(t):
    t = t.tag
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


class XMLDumpPageParser(object):
    def __init__(self):
        self.isPage = False
        self.pages = []
        self.current_page = {}
        self.path_stack = []
        self.ignored_tags = {'mediawiki', 'page'}
        
    def handle(self, event, element):
        stipped_elem = strip_tag_name(element)

        self._start_event_hook(event, element)
        
        if event == "start" and stipped_elem == "page":
            self.isPage = True
        elif event == "end" and stipped_elem == "page":
            self.isPage = False
            self._finalize_page()
        
        if event == "end" and self.isPage:
            self._parse_page_element(element)
        
        self._end_event_hook(event, element)

    def _end_event_hook(self, event, element):
        stripped = strip_tag_name(element)
        if event == 'end' and stripped not in self.ignored_tags:
            if self.path_stack[-1] == stripped:
                self.path_stack = self.path_stack[:-1]
            else:
                print("Panic attack. Unhandled event")
                raise RuntimeError("Unhandled event")

    def _start_event_hook(self, event, element):
        stripped = strip_tag_name(element)
        if event == 'start' and stripped not in self.ignored_tags:
            self.path_stack.append(stripped)
    
    def get_pages(self):
        return self.pages

    def _finalize_page(self):
        self.pages.append(self.current_page)
        self.current_page = {}
    
    def _parse_page_element(self, element):
        stripped_tag = strip_tag_name(element) #This element is also last item of path stack. 
        if element.text:
            texts = [element.text.strip()] + [child.tail.strip() for child in element if child.tail]
            final_tag_path = ".".join(self.path_stack)
            self.current_page[final_tag_path] = " ".join(texts)


class PageDataORESLoader(object):

    def __init__(self, pages_df, lang="en"):
        self.df = pages_df
        self.lang = lang

    def load(self, chunk_size=50):
        ores_scores = []
        for idx in tqdm.tqdm(range(0, len(self.df), chunk_size)):
            revisions = self.df.iloc[idx:idx+chunk_size]['revision.id'].values
            scores = fetch_ores_score(revisions, self.lang)
            ores_scores.append(scores)
        self.scores = pd.concat(ores_scores)
        return self

    def get_pages_with_ores(self):
        return pd.merge(self.df, self.scores, left_on='revision.id', right_index=True)
        

def process_dumps(xml_folder_path, output_path):
    xml_files = [file for file in os.listdir(xml_folder_path) if '.xml' in file]
    print(f"XML Files found: " + ",".join(xml_files))
    for xml_file in xml_files:
        print(f"Processing {xml_file}")
        dump_parser = XMLDumpPageParser()
        for event, elem in tqdm.tqdm(etree.iterparse(f"{xml_folder_path}/{xml_file}", events=('start', 'end'))):
            dump_parser.handle(event, elem)
        
        pages = pd.DataFrame(dump_parser.get_pages())
        ores_loader = PageDataORESLoader(pages).load()
        
        pages_with_scores = ores_loader.get_pages_with_ores()

        output_file = xml_file.split('.')[0] + "_raw.csv"
        pages_with_scores.to_csv(f"{output_path}/{output_file}")


if __name__ == "__main__":
    
    XML_DIR = "data/xml"
    CSV_DIR = "data/csv"

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--xml-dir", type=str, default=XML_DIR)
    parser.add_argument("--csv-dir", type=str, default=CSV_DIR)
    args = parser.parse_args()
    
    process_dumps(args.xml_dir, args.csv_dir)
