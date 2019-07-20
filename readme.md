*Work In Progress until 22/07/2019*
# Diversity of Wikipedia Article References
## Final project of Mining Massive Datasets course, July 2019

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
Run 
```
python ./test.py -t {your favourite english Wiki article}
```

For example
```
python ./test.py -t "Principal component analysis"
```
will output:
```
- the page is well cited and argumented
- the references distribution:
  > 55%: scientiﬁc papers
  > 40%: books
  > 5%: software documentation
- this is likely a featured article
```

### Train and test on sample data
Run `Pipeline.ipynb`.
The notebook represents all the pipeline stages with a small wiki dump. It allocates about 50MB of memory. All the data is written in `sample_data` directory. 

### Run full pipeline

#### Train
From root project folder run:

- `./load_wiki_data_full.sh` (loads full [2019-07-01 Wikipedia](https://dumps.wikimedia.org/enwiki/20190701/) data * )
- `python 1_data_collection/xml_to_csv.py` (parses XML to CSV with streaming XML parser; fetches ORES assessments)
- `python 2_feature_engineering/csv_to_features.py` (filters articles; transfroms to features)
- `python 3_modeling/features_to_clusters.py` (clusters the data using features; evaluates ans saves the model for testing)

\* If you want to download only subsample, edit the list of dumps in `data/dump_names.txt`.

#### Test
See [Test our estimator](#test-our-estimator)
