# Diversity of Wikipedia article references (July 2019)
## Mining Massive Datasets final project (WIP until 22/07/2019)

Authors:

* Sasha Chepurnoi
* Philipp Kofman
* Vadym Korshunov
* Yaroslava Lochman

## Project Description


See our report in `Report.ipynb`.

## Project Pipeline

### Set up environment

```
virtualenv .env --python=python3
source .env/bin/activate
pip install -r requirements.txt
```

### Test our estimator
Run `python ./test.py -t "Margaret Hamilton (software engineer)"`

### Train and test on sample data
Run `Pipeline.ipynb`.
The notebook represents all the pipeline stages with a small wiki dump. It allocates about 50MB of memory. All the data is written in `sample_data` directory. 

### Run full pipeline

#### Train
From root project folder run:

- `./load_wiki_data_full.sh` (loads full [2019-07-01 Wikipedia](https://dumps.wikimedia.org/enwiki/20190701/) data * )
- `python 1_data_collection/xml_to_csv.py`
- `python 2_feature_engineering/csv_to_features.py`
- `python 3_modeling/features_to_clusters.py`

\* If you want to download only subsample edit `load_wiki_data_full.sh`:

Replace:
```
cat data/parts.txt | while read bz_dump_name; do
```
With:
```
# 3 can be any number of dumps that you want. 
# Also you can use head/tail or any bash filtering here.

cat data/parts.txt | tail -n -3 | while read bz_dump_name; do
```

#### Test
Run `python test.py -t "Margaret Hamilton (software engineer)"`