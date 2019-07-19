from pyspark.sql import *
from pyspark.sql.functions import col, lower, regexp_replace, split, size, UserDefinedFunction
from pyspark.sql.types import StringType, IntegerType
from functools import reduce
import re

def rename_columns(df):
    for c in df.columns:
        if "revision." in c:
            df = df.withColumnRenamed(c, c[len("revision."):])
    return df

def filter_columns(df, print_columns=False):
    """Columns filtering
        Useful: sha1 (as identifier),  timestamp, title, text
        Questionable: user, comment, ip, id (there are different articles with the same id), parentid, restrictions
        Not useful (no unique info): model, format, ns, contributor, revision, restrictions
    """
    ores_weights = {'Stub': 1, 'Start': 2, 'C': 3, 'B': 4, 'GA': 5, 'FA': 6}
    ores_scores = list(ores_weights.keys())
    useful_columns = ["sha1", "timestamp", "title", "text"] + ores_scores
    if print_columns:
        print("All columns:", df.columns)
        print("Unique values for..")
        for column in ["format", "model", "ns", "contributor", "revision", "restrictions"]:
            print("\t", column, ":", df.select(column).distinct().rdd.map(lambda r: r[0]).collect())
        print("Useful columns:", useful_columns)
    return df[useful_columns]

def words_counts(df):
    return df.withColumn('n_words', size(split(col('text'), ' ')))

def single_head_level_count(text, level):
    assert level in range(2,7)
    pattern = "=" * level
    pattern = pattern + "[a-zA-Z0-9.,!? ]+" + pattern
    return size(split(text, pattern=pattern))-1

def count_headings(df):
    """Headings counting
    Syntaxis:
        ==Level 2==
        ===Level 3===
        ====Level 4====
        =====Level 5=====
        ======Level 6======
    """
    return reduce(
        lambda df, level: df.withColumn("level{}".format(level),
                                        single_head_level_count(col("text"), level)),
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
    # filter the basic wikipedia syntaxis
    pattern_filtering = '\n\n\{\{.*\}\}\n\n|\n\n\[\[.*\]\]\n\n|\n\n={1,7}.*={1,7}\n\n'
    # split by two enters
    pattern_splitting = '\n\n'
    return df.withColumn('n_paragraphs', size(split(regexp_replace(col('text'), 
                                                                   pattern_filtering, ''), 
                                                    pattern_splitting))-1)

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


def extract_features(df_features, filter=True):
    df_features = filter_columns(rename_columns(df_features))

    df_features = words_counts(df_features)
    df_features = count_headings(df_features)

    # citation counter ver1
    book_citations_count = UserDefinedFunction(citation_counter("book"), IntegerType())
    journal_citations_count = UserDefinedFunction(citation_counter("journal"), IntegerType())
    df_features = df_features.withColumn("book_citations", book_citations_count("text"))\
                    .withColumn("journal_citations", journal_citations_count("text"))

    # citation counter ver2
    # df_features = citation_counter2("book")(df_features)
    # df_features = citation_counter2("journal")(df_features)

    df_features = count_internal_links(df_features)
    df_features = count_external_links(df_features)
    df_features = count_paragraphs(df_features)
    df_features = count_unreferenced(df_features)
    df_features = count_categories(df_features)
    df_features = count_of_images(df_features)

    if filter:
        features_names = ['Stub', 'Start', 'C', 'B', 'GA', 'FA',
                        'n_words', 'n_internal_links', 'n_external_links',
                        'level2', 'level3', 'level4', 'level5', 'level6',
                        'book_citations', 'journal_citations',
                        'n_paragraphs', 'n_unreferenced', 'n_categories', 'n_images']

        df_features = df_features.select(list(map(lambda x: df_features[x].cast('double') if x != 'title' else df_features[x], 
                                                features_names)))

        for feature in features_names:
            df_features = df_features.filter(df_features[feature].isNotNull())

    return df_features