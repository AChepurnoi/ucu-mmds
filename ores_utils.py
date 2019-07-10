import pandas as pd
import requests

def score2sum(score_doc, weights):
    if score_doc is None:
        return None
    weighted_sum = 0
    for cl, proba in score_doc['probability'].items():
        weighted_sum += weights[cl] * proba
    return weighted_sum

def fetch_wp10_score(rev_ids, lang):
    wiki = "{}wiki".format(lang)
    response = requests.get("http://ores.wmflabs.org/v3/scores/{}/?models=draftquality|wp10&revids={}".format(wiki, '|'.join(rev_ids)))
    try:
        scores_df = pd.DataFrame(
            [response.json()[wiki]['scores'][str(rev_id)]['wp10']['score']['probability']
                for rev_id in rev_ids], index=rev_ids)
        return scores_df
    except:
        return None