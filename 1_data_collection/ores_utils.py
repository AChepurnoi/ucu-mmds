import pandas as pd
import requests
import numpy as np
from code_timer import CodeTimer

def score2sum(score_doc, weights):
    if score_doc is None:
        return None
    weighted_sum = 0
    for cl, proba in score_doc['probability'].items():
        weighted_sum += weights[cl] * proba
    return weighted_sum

def fetch_ores_score(revision_ids, lang):
    wiki = "{}wiki".format(lang)
    # with CodeTimer("ORES HTTP Request"):
    response = requests.get("http://ores.wmflabs.org/v3/scores/{}/?models=draftquality|wp10&revids={}".format(wiki, '|'.join(revision_ids)))
    try:
        scores_df = pd.DataFrame(
            [response.json()[wiki]['scores'][str(rev_id)]['wp10']['score']['probability']
                for rev_id in revision_ids], index=revision_ids)
        return scores_df
    except Exception as e:
        print(f"Alert: Exception fetching ORES score for revisions. Ex: {e}")
        return pd.DataFrame(np.nan, index=revision_ids, columns=['B', 'C', 'FA', 'GA', 'Start', 'Stub'])