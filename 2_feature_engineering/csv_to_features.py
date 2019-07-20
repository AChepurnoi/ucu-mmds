import os
from glob import glob

import findspark
try:
    findspark.init()
except:
    PYSPARK_PATH = '../spark/spark-2.4.3-bin-hadoop2.7/' # change path to yours
    findspark.init(PYSPARK_PATH)
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from functions import *

def read_df(df_paths):
    df = spark.read.csv(df_paths, inferSchema=True, header=True, multiLine=True, escape='"').drop('revision.id')
    for c in df.columns:
        if "revision." in c:
            df = df.withColumnRenamed(c, c[len("revision."):])
    return df

def rename_columns(df):
    for c in df.columns:
        if "revision." in c:
            df = df.withColumnRenamed(c, c[len("revision."):])
    return df

def filter_columns(df, print_columns=False):
    """Columns filtering
        Useful: title (as identifier), text
        Questionable: user, comment, ip, id (there are different articles with the same id), parentid, restrictions, timestamp, sha1
        Not useful (no unique info): model, format, ns, contributor, revision, restrictions
    """
    ores_weights = {'Stub': 1, 'Start': 2, 'C': 3, 'B': 4, 'GA': 5, 'FA': 6}
    ores_scores = list(ores_weights.keys())
    useful_columns = ["title", "text"] + ores_scores
    if print_columns:
        print("All columns:", df.columns)
        print("Unique values for..")
        for column in ["format", "model", "ns", "contributor", "revision", "restrictions"]:
            print("\t", column, ":", df.select(column).distinct().rdd.map(lambda r: r[0]).collect())
        print("Useful columns:", useful_columns)
    return df[useful_columns]

def filter_articles(df):
    df = df.filter(~lower(df.text).rlike('^#redirect'))
    df = df.filter(~lower(df.text).rlike('\{\{(disambig|[a-z0-9 |]*disambiguation[|a-z0-9 ]*)\}\}'))
    df = df.filter(~lower(df.title).rlike('^template:'))
    df = df.filter(~lower(df.title).rlike('^category:'))
    df = df.filter(~lower(df.title).rlike('^file:'))
    df = df.filter(~lower(df.title).rlike('^wikipedia:'))
    df = df.filter(~lower(df.title).rlike('^portal:'))
    df = df.filter(~lower(df.title).rlike('^help:'))
    return df

def create_features(csv_dir, date, debug=False, lim=None):
    df_paths = glob(os.path.join(csv_dir, "enwiki-{}-pages-articles-multistream*_raw.csv".format(date)))
    
    df = read_df(df_paths)
    df_features = filter_columns(df)
    df_features = filter_articles(df_features)
    if lim is not None:
        df_features = df_features.limit(lim)
        df_out_path = os.path.join(csv_dir, "enwiki-{}-features-small.csv".format(date))
    else:
        df_out_path = os.path.join(csv_dir, "enwiki-{}-features.csv".format(date))

    df_features = extract_features(df_features, debug=debug)

    df_features.printSchema()
    pdf_features = df_features.toPandas()
    print("Size of the DataFrame: {} records".format(len(pdf_features)))
    pdf_features.to_csv(df_out_path)
    print("Features saved to {}".format(df_out_path))
    return df_features, pdf_features

if __name__ == "__main__":

    CSV_DIR = "../data/csv"
    DATE = "20190701"

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-dir", type=str, default=CSV_DIR)
    parser.add_argument("--date", type=str, default=DATE)
    args = parser.parse_args()

    create_features(args.csv_dir, args.date)