import os
import argparse
import sys
import findspark
try:
    findspark.init()
except:
    PYSPARK_PATH = '../spark/spark-2.4.3-bin-hadoop2.7/'
    # change path to yours
    findspark.init(PYSPARK_PATH)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import UserDefinedFunction

import sys
stages = ["1_data_collection", "2_feature_engineering", "3_modeling", "4_evaluation"]
for stage in stages:
    sys.path.insert(0, stage)

from functions import tag_article, extract_features

import bs4
import requests
import re
import numpy as np
import pandas as pd

def get_raw_article(title):
    # title formatting
    title = re.sub('^ {1,}| {1,}$', '', title)
    title = re.sub(' {1,}', '_', title)
    # get link with wiki raw text with syntax
    link = 'https://en.wikipedia.org/w/index.php?title={title}&action=edit'.format(title=title)
    # parse page and find the needed field
    response = requests.get(link)
    html = bs4.BeautifulSoup(response.text, 'html.parser')
    # get text
    try:
        text = html.select('textarea')[0].contents[0]
        return text
    except IndexError:
        print('Error! Try to rewrite your arctile title (maybe you did mistake)')
        return None

def get_revision_id(title):
    # title formatting
    title = re.sub('^ {1,}| {1,}$', '', title)
    title = re.sub(' {1,}', '_', title)
    # get link with wiki raw text with syntax
    link = 'https://en.wikipedia.org/w/index.php?title={title}&action=history'.format(title=title)
    # parse page and find the needed field
    response = requests.get(link)
    html = bs4.BeautifulSoup(response.text, 'html.parser')
    # get the lastest revision
    tag = html.findAll(lambda tag: tag.name == "li" and "data-mw-revid" in tag.attrs)[0]
    revision_id = int(tag.attrs['data-mw-revid'])
    return revision_id

def get_ores_score(revision_id, lang="en"):
    wiki = "{}wiki".format(lang)
    # with CodeTimer("ORES HTTP Request"):
    response = requests.get("http://ores.wmflabs.org/v3/scores/{}/?models=draftquality|wp10&revids={}".format(wiki, revision_id))
    try:
        scores = response.json()[wiki]['scores'][str(revision_id)]['wp10']['score']['probability']
        return scores
    except Exception as e:
        print(f"Alert: Exception fetching ORES score for revision. Ex: {e}")
        return None

def get_features(ores_scores, text, title):
    # title formatting
    title = re.sub('^ {1,}| {1,}$', '', title)
    title = re.sub(' {1,}', ' ', title)
    # format output dict
    ores_scores['title'] = title
    ores_scores['text'] = text
    # get dataframe
    pandas_df = pd.DataFrame.from_records([ores_scores])
    # convert to pyspark dataframe
    df = spark.createDataFrame(pandas_df.astype(str))
    df = tag_article(df)
    pyspark_features = extract_features(df, filter=True, debug=False)
    pdf = pyspark_features.toPandas()
    # get pandas data
    return pdf

ores_scores_dict = {
    0: 'Stub',
    1: 'Start',
    2: 'C',
    3: 'B',
    4: 'GA',
    5: 'FA'
}

ores_to_descr = {
    'Stub': '- this article provides very little meaningful content',
    'Start': '- this article provised some meaningful content, but most readers will need more',
    'C': '- this article has a middle level of content and references',
    'B': '- this article has a good amount of content and references',
    'GA': '- this article very useful for readers: a fairly complete treatment of the subject',
    'FA': '- this is featured article: professional, outstanding and thorough'
}

clusters_to_descr = {
    0: '- the page is pretty well cited and argumented',
    1: '- the page is medium cited and argumented',
    2: '- the page is poorly cited and argumented'
}

references_to_descr = {
    0: 'books',
    1: 'scientific papers (journals, publications, etc)',
    2: 'internet resources (news, archive, etc)',
    3: 'media materials (prints)'
}

tag_to_descr = {
    "redirect": "It's a redirecting page. Please make another request: ",
    "disambig": "It's a disambiguation. Please specify the request: ",
    "template": "It's a template. Please make another request: ",
    "category": "It's a category page. Please make another request: ",
    "file": "It's a file. Please make another request: ",
    "wikipedia_related": "It's a wikipedia project page. Please make another request: ",
    "portal": "It's a portal. Please make another request: ",
    "help": "It's a help page. Please make another request: "
}

def formatted_output(pandas_features):
    if pandas_features[tag_to_descr.keys()].any(axis=1).iloc[0]:
        new_title = input(tag_to_descr[pandas_features[tag_to_descr.keys()].idxmax(axis=1).iloc[0]])
        test_article(new_title)
        return
    output = ''

    # compute references distribution and create template
    references = ['book_citations',
                  'journal_citations',
                  'web_citations',
                  'news_citations']
    references_distr = pandas_features[references].values
    indexes = (-references_distr).argsort()[0]
    references_distr = 100 * references_distr[0] / references_distr.sum()

    output += '- the references distribution:\n'
    for index in indexes:
        output += '  > {perc:4.0f}% {d}\n'.format(perc=references_distr[index], d=references_to_descr[index])

    # add row with ores info
    ores_max_index = pandas_features[['Stub', 'Start', 'C', 'B', 'GA', 'FA']].values.argmax()
    output += '{}'.format(ores_to_descr[ores_scores_dict[ores_max_index]])

    return output

def test_article(title):
    text = get_raw_article(title)
    if text == None:
        sys.exit()

    revision_id = get_revision_id(title)
    ores_score = get_ores_score(revision_id)
    pandas_features = get_features(ores_score, text, title)
    output = formatted_output(pandas_features)
    if output is not None:
        print(output)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--title", type=str, required=True)
    args = parser.parse_args()

    test_article(args.title)