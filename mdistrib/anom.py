
import numpy as np
import math
from mdistrib.edgehash import Edgehash
from dask import delayed

def compute_score_partitions(i,gf, anom_score, cur_count, tot_count):
    anom_score = 0

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



def mdistrib(df, num_rows, num_buckets, num_partitions):

    output = []
    anom_score = []
    ro = 0
    m = df.src.max()
    totalIterations = list(set(df["timestamp"]))[-1:][0]
    df = df.set_index('timestamp')

    df = df.repartition(npartitions=totalIterations)

    while(ro < totalIterations):
        cur_count = [Edgehash(num_rows, num_buckets, m)]*num_partitions
        total_count = Edgehash(num_rows, num_buckets, m)
        for part in range(num_partitions):
            gf = df.get_partition(part+ro)
            a = (delayed)(compute_score_partitions)(part+ro, gf, anom_score, cur_count[part], total_count)
            output.append(a)
            
        ro += num_partitions



