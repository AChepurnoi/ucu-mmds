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

from functions import *

CSV_DIR = "../data/csv"
DATE = "20190701"

def get_clusters(csv_dir, date):
    df_path = os.path.join(csv_dir, "enwiki-{}-features.csv".format(date))
    pass

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-dir", type=str, default=CSV_DIR)
    parser.add_argument("--date", type=str, default=DATE)
    args = parser.parse_args()

    get_clusters(args.csv_dir, args.date)