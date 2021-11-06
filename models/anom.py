
import numpy as np
import math
from models.CMSUtilities.edgehash import Edgehash
from dask import delayed
from datasketches import frequent_strings_sketch

from models.CMSUtilities.score_partitions import compute_score_partitions
from models.FISUtilities.score_partitions import compute_score_partitions_fis


def mdistribCMS(df, num_rows, num_buckets, num_partitions):

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
            a = (delayed)(compute_score_partitions)(
                part+ro, gf, anom_score, cur_count[part], total_count)
            output.append(a)

        ro += num_partitions
    return output

def mdistribFIS(df, K, num_partitions):

    output = []
    anom_score = []
    ro = 0
    m = df.src.max()
    totalIterations = list(set(df["timestamp"]))[-1:][0]
    df = df.set_index('timestamp')

    df = df.repartition(npartitions=totalIterations)

    while(ro < totalIterations):
        cur_count = [frequent_strings_sketch(K).serialize()]*num_partitions
        total_count = frequent_strings_sketch(K).serialize()
        for part in range(num_partitions):
            gf = df.get_partition(part+ro)
            a = (delayed)(compute_score_partitions_fis)(
                part+ro, gf, anom_score, cur_count[part], total_count)
            output.append(a)
        ro += num_partitions
    return output
