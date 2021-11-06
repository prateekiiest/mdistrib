import numpy as np
import math

def compute_score_partitions(i,gf, anom_score, cur_count, tot_count):
    """
    """
    for row in gf.itertuples():
        cur_src = int(row.src) 
        cur_dst = int(row.dst) 
        cur_count.insert(cur_src, cur_dst, 1)
        tot_count.insert(cur_src, cur_dst, 1)
        cur_t = i+1
        cur_mean = tot_count.get_count(cur_src, cur_dst) / cur_t
        sqerr = np.power(cur_count.get_count(cur_src, cur_dst) - cur_mean, 2)
        cur_score = 0 if cur_t == 1 else sqerr / cur_mean + sqerr / (cur_mean * (cur_t - 1))
        cur_score = 0 if math.isnan(cur_score) else cur_score
        anom_score.append(sqerr)
    return anom_score