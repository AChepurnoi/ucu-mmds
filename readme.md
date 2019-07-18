
## MMDS Final project

### Loading Data

Data source: https://dumps.wikimedia.org/enwiki/20190701/

#### Full dataset
Downloading full list of wiki dumps: run `./load_wiki_data_full.sh` from root project folder. 

#### Subsample
If you want to download only subsample edit `load_wiki_data_full.sh`:

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



### Setting up environment

```
virtualenv .env --python=python3
source .env/bin/activate
pip install -r requirements.txt
```
