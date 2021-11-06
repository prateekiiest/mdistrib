
PARTITION_NUM = 19
import math
num_rows=2
num_buckets=769
m = pf.src.max()

def compute_score(i,gf, anom_score, cur_count, tot_count):
    #k = gf["src"] + '-' + gf["dest"]
    anom_score = 0

    for row in gf.itertuples():
        cur_src = int(row.src) 
        cur_dst = int(row.dst) 
        cur_count.insert(cur_src, cur_dst, 1)
        total_count.insert(cur_src, cur_dst, 1)
        cur_t = i+1
        cur_mean = total_count.get_count(cur_src, cur_dst) / cur_t
        sqerr = np.power(cur_count.get_count(cur_src, cur_dst) - cur_mean, 2)
        cur_score = 0 if cur_t == 1 else sqerr / cur_mean + sqerr / (cur_mean * (cur_t - 1))
        cur_score = 0 if math.isnan(cur_score) else cur_score
        #print(cur_src)
        gf["label"].values[items] = sqerr
        anom_score.append(0)
    return anom_score

def read_partition(i, gf, anom_score, cur_count):
    #k = gf["src"] + '-' + gf["dest"]
    anom_score = 0

    for row in gf.itertuples():
        cur_src = int(row.src) 
        cur_dst = int(row.dst) 
        cur_count.insert(cur_src, cur_dst, 1)
        #total_count.insert(cur_src, cur_dst, 1)
        #cur_t = i+1
        #cur_mean = total_count.get_count(cur_src, cur_dst) / cur_t
        #sqerr = np.power(cur_count.get_count(cur_src, cur_dst) - cur_mean, 2)
        #cur_score = 0 if cur_t == 1 else sqerr / cur_mean + sqerr / (cur_mean * (cur_t - 1))
        #cur_score = 0 if math.isnan(cur_score) else cur_score
        #print(cur_src)
        #gf["label"].values[items] = 1.0
        
        #anom_score.append(0)
    return anom_score

output = []
anom_score = []
ro = 0

while(ro < 1463):
    cur_count = [Edgehash(num_rows, num_buckets, m)]*PARTITION_NUM
    #total_count = Edgehash(num_rows, num_buckets, m)
    for part in range(PARTITION_NUM):
        gf = df.get_partition(part+ro)
        a = (delayed)(read_partition)(part+ro, gf, anom_score, cur_count[part])
        output.append(a)
        
    ro += PARTITION_NUM



