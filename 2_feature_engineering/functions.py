"""Each funcation here extracts some feature from text
"""

from pyspark.sql import *
from pyspark.sql.functions import col, lower, regexp_replace, split, array_remove, size, UserDefinedFunction
from pyspark.sql.types import StringType, IntegerType
from functools import reduce
import re

eps = 1e-8

def tag_article(df):
    df = df.withColumn('redirect', lower(df.text).rlike('^#redirect'))
    df = df.withColumn('disambig', lower(df.text).rlike('\{\{(disambig|[a-z0-9 |]*disambiguation[|a-z0-9 ]*)\}\}'))
    df = df.withColumn('template', lower(df.title).rlike('^template:'))
    df = df.withColumn('category', lower(df.title).rlike('^category:'))
    df = df.withColumn('file', lower(df.title).rlike('^file:'))
    df = df.withColumn('wikipedia_related', lower(df.title).rlike('^wikipedia:'))
    df = df.withColumn('portal', lower(df.title).rlike('^portal:'))
    df = df.withColumn('help', lower(df.title).rlike('^help:'))
    return df

def words_counts(df):
    return df.withColumn('n_words', size(split(col('text'), ' ')))

def count_headings(df):
    """Headings counting
    Syntaxis:
        ==Level 2==
        ===Level 3===
        ====Level 4====
        =====Level 5=====
        ======Level 6======
    """
    def _single_head_level_count(text, level):
        assert level in range(2,7)
        pattern = "=" * level
        pattern = pattern + "(?!.*[Ll]inks)(?!References)[a-zA-Z0-9.,!? ]+" + pattern
        return size(split(text, pattern=pattern))-1
        
    return reduce(
        lambda df, level: df.withColumn("level{}".format(level),
                                        _single_head_level_count(col("text"), level)),
        range(2,7), df)

def citation_counter(citation_source):
    """Citation counting
    Syntaxis:
        {{cite {book}(.*?)}}
        {{cite {journal}(.*?)}}
    """
    def _count_citations(text):
        matches = re.findall(f"{{cite {citation_source}(.*?)}}", str(text), re.IGNORECASE)
        return len(matches)
    return _count_citations

def citation_counter2(citation_source):
    """Citation counting
    Syntaxis:
        {{cite {book}(.*?)}}
        {{cite {journal}(.*?)}}
    """
    def _count_citations(df):
        pattern = "\{cite "+citation_source+"(.*?)\}"
        return df.withColumn("{}_citations".format(citation_source),\
                size(split(col('text'), pattern=pattern))-1)
    return _count_citations

def count_internal_links(df):
    """Internal Links:
        [[A]] -- internal reference to an article titled A
        [[A|B]] -- internal reference to an article titled A (written as B)
        [[A#C|B]] -- internal reference to a section C of an article titled A (written as B)
    """
    pattern = "\[\[[a-zA-Z0-9.,!? ]+\]\]"
    pattern += "|\[\[[a-zA-Z0-9.,!? ]+\|[a-zA-Z0-9.,!? ]+\]\]"
    pattern += "|\[\[[a-zA-Z0-9.,!? ]+#[a-zA-Z0-9.,!? ]+\|[a-zA-Z0-9.,!? ]+\]\]"
    return df.withColumn("n_internal_links", size(split(col('text'), pattern=pattern))-1)

def count_external_links(df):
    """External Links:
        https://www.google.com -- simple link
        [https://www.google.com] -- link (reference)
        [https://www.google.com A] -- reference written as A
        <ref name="B">[https://www.google.com A]</ref> -- reference A written as B, can be referenced again like:
        <ref name="B" /> -- reference to the source B
        <ref>Lots of words</ref> -- reference without a link
        {{sfnm|1a1=Craig|1y=2005|1p=14|2a1=Sheehan|2y=2003|2p=85}} -- external reference
        Example:
            {{sfnm|1a1=McLaughlin|1y=2007|1p=59|2a1=Flint|2y=2009|2p=27}} -- McLaughlin 2007, p. 59; Flint 2009, p. 27.
            {{sfnm|1a1=Craig|1y=2005|1p=14|2a1=Sheehan|2y=2003|2p=85}} -- Craig 2005, p. 14; Sheehan 2003, p. 85.
    """
    pattern = 'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    pattern += '|\[https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+\]'
    pattern += '|\[https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+\ [a-zA-Z0-9.,!? ]+]'
    pattern += '<ref name="[a-zA-Z0-9.,!? ]+">\[https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+\]'
    return df.withColumn("n_external_links",
                         size(split(col('text'), pattern=pattern))-1)

def count_paragraphs(df):
    """Paragraphs
    """
    pattern_filterings = [
                          '\n\n+(\{\{[^\}]*\}\}|\[\[[^\]]*\]\]|={1,7}.*={1,7})\n\n+',
                          '==References==[\s\S]*',
                          '==External [lL]inks==[\s\S]*',
                          '\{\{([^\{\}]*(\{\{.*\}\})*)*\}\}\n+',
                          '\{\{([^\{\}]*(\{\{.*\}\})*)*\}\}$',
                          '\[\[[^\]]*\]\]\n+',
                          '\[\[[^\]]*\]\]$',
                          '\n\n+'
                         ]
    c = col('text')
    for pattern in pattern_filterings:
        c = regexp_replace(c, pattern, '\n\n')
    splitted = array_remove(split(c, '\n\n'), '')
    # return df.withColumn('paragraphs', splitted).withColumn('n_paragraphs', size(splitted))
    return df.withColumn('n_paragraphs', size(splitted)-1)

def count_unreferenced(df):
    """
        <ref>Lots of words</ref> -- reference without a link
        {{cn}} -- citation needed
    """
    pattern = '\{\{cn\}\}|<ref>[a-zA-Z0-9.,!? ]+</ref>'
    return df.withColumn('n_unreferenced', size(split(col('text'), pattern))-1)

def count_categories(df):
    """
        [[Category:Category name]]
        [[:Category:Category name]]
        [[:File:File name]]
    """
    pattern = '\[\[:?Category:[a-zA-Z0-9.,\-!?\(\) ]+\]\]'
    return df.withColumn('n_categories', size(split(col('text'), pattern))-1)


def count_of_images(df):
    """
        [[File: | thumb  | upright | right | alt= | caption ]]
    """
    any_text = "[a-zA-Z0-9.,!? ]+ \] "
    pattern = "\[[a-zA-Z0-9.,!? ]+\|[a-zA-Z0-9.,!? ]+\|[a-zA-Z0-9.,!? ]+\|[a-zA-Z0-9.,!? ]+\|[a-zA-Z0-9.,!? ]+\|[a-zA-Z0-9.,!? ]+\|[a-zA-Z0-9.,!? ]+\|[a-zA-Z0-9.,!? ]+\]"
    return df.withColumn("n_images", size(split(col('text'), pattern=pattern))-1)
            
def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

def extract_features(df_features, filter=True, debug=False, ores=True):
    if debug:
        df_features = df_features.limit(50)

    df_features = words_counts(df_features)
    df_features = count_headings(df_features)

    book_citations_count = UserDefinedFunction(citation_counter("book"), IntegerType())
    journal_citations_count = UserDefinedFunction(citation_counter("journal"), IntegerType())
    web_citations_count = UserDefinedFunction(citation_counter("web"), IntegerType())
    news_citations_count = UserDefinedFunction(citation_counter("news"), IntegerType())
    df_features = df_features\
                    .withColumn("book_citations", book_citations_count("text"))\
                    .withColumn("journal_citations", journal_citations_count("text"))\
                    .withColumn("web_citations", web_citations_count("text"))\
                    .withColumn("news_citations", news_citations_count("text"))

    df_features = count_internal_links(df_features)
    df_features = count_external_links(df_features)
    df_features = count_paragraphs(df_features)

    df_features = df_features.withColumn("average_internal_links", (col("n_internal_links")  / (col("n_paragraphs") + eps)))
    df_features = df_features.withColumn("average_external_links", (col("n_external_links")  / (col("n_paragraphs") + eps)))

    df_features = count_unreferenced(df_features)
    df_features = count_of_images(df_features)

    if not debug:
        if filter:
            labels = ['Stub', 'Start', 'C', 'B', 'GA', 'FA'] if ores else []
            tags = ["redirect", "disambig", "template", "category", "file", "wikipedia_related", "portal", "help"] \
                   if has_column(df_features, "redirect") else [] 
            features_names = ['title',
                            'n_words', 'n_internal_links', 'n_external_links',
                            'level2', 'level3', 'level4', 'level5', 'level6',
                            'book_citations', 'journal_citations', 'web_citations', 'news_citations',
                            'average_external_links', 'average_internal_links',
                            'n_paragraphs', 'n_unreferenced', 'n_images'] + labels + tags

            df_features = df_features.select(list(map(lambda x: df_features[x].cast('double') if x != 'title' else df_features[x],
                                                    features_names)))

            for feature in features_names:
                df_features = df_features.filter(df_features[feature].isNotNull())

    return df_features
