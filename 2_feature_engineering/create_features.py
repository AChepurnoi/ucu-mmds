import os
from glob import glob
import numpy as np

import findspark
try:
    findspark.init()
except:
    PYSPARK_PATH = '../spark/spark-2.4.3-bin-hadoop2.7/' # change path to yours
    findspark.init(PYSPARK_PATH)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import UserDefinedFunction

from functions import *

CSV_DIR = "../data/csv"
DATE = "20190701"

def main(csv_dir, date):
    df_paths = glob(os.path.join(csv_dir, "enwiki-{}-pages-articles-multistream*_raw.csv".format(DATE)))
    df = spark.read.csv(df_paths, inferSchema=True, header=True, multiLine=True, escape='"')
    df_features = filter_columns(df)
    df_features.printSchema()
    print("Size of the DataFrame: {} records".format(df_features.count()))

    df_features = words_counts(df_features)

    df_features = count_headings(df_features)

    book_citations_count = UserDefinedFunction(citation_counter("book"), IntegerType())
    journal_citations_count = UserDefinedFunction(citation_counter("journal"), IntegerType())
    df_features = df_features.withColumn("book_citations", book_citations_count("text"))\
    .withColumn("journal_citations", journal_citations_count("text"))

    df_features = count_internal_links(df_features)

    df_features = count_external_links(df_features)

    df_features = count_paragraphs(df_features)

    df_features = count_unreferenced(df_features)

    df_features = count_categories(df_features)

    df_features = count_of_images(df_features)

    features_names = ['title',
                    'Stub', 'Start', 'C', 'B', 'GA', 'FA',
                    'n_words', 'n_internal_links', 'n_external_links',
                    'level2', 'level3', 'level4', 'level5', 'level6',
                    'book_citations', 'journal_citations',
                    'n_paragraphs', 'n_unreferenced', 'n_categories', 'n_images'
                    ]

    df_features = df_features.select(list(map(lambda x: df_features[x].cast('double') if x != 'title' else df_features[x], 
                                            features_names)))

    for feature in features_names:
        df_features = df_features.filter(df_features[feature].isNotNull())

    df_features.printSchema()
    df_out_path = os.path.join(csv_dir, "enwiki-{}-features.csv".format(DATE))
    df_features.toPandas().to_csv(df_out_path)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-dir", type=str, default=CSV_DIR)
    parser.add_argument("--date", type=str, default=DATE)
    args = parser.parse_args()
    main(args.csv_dir, args.date)