import numpy as np
import math
from datasketches import frequent_strings_sketch

def compute_score_partitions_fis(i,gf, anom_score, cur_count, tot_count):
    """
    """

    fe = frequent_strings_sketch.deserialize(cur_count)
    fs = frequent_strings_sketch.deserialize(tot_count)
    for row in gf.itertuples():
        cur_src = int(row.src) 
        cur_dst = int(row.dst) 
        cur_edge = str(cur_src) + '-' + str(cur_dst)
        fe.insert(cur_edge)
        fs.insert(cur_edge)
        cur_t = i+1
        cur_mean = fs.get_count(cur_edge) / cur_t
        sqerr = np.power(fe.get_count(cur_edge) - cur_mean, 2)
        cur_score = 0 if cur_t == 1 else sqerr / cur_mean + sqerr / (cur_mean * (cur_t - 1))
        cur_score = 0 if math.isnan(cur_score) else cur_score
        anom_score.append(sqerr)
    return anom_score