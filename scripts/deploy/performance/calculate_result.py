# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.    

import math
import json
import os
import re
import sys

total = 0

WEIGHT_RE = re.compile(r"active_weights:\[([^\]]*)\]")

class ParsedLog:
    def __init__(self):
        self.tps = []
        self.lat = []
        self.lat2 = []
        self.lat3 = []
        self.lat4 = []
        self.before_threshold_tps = []
        self.after_threshold_tps = []
        self.threshold_seen = False

    def __iter__(self):
        yield self.tps
        yield self.lat
        yield self.lat2
        yield self.lat3
        yield self.lat4

def valid_positive_number(value):
    return math.isfinite(value) and value > 0

def parse_positive_float(raw):
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if valid_positive_number(value) else None

def parse_active_weights(line):
    match = WEIGHT_RE.search(line)
    if not match:
        return None
    weights = []
    for raw in match.group(1).split(','):
        raw = raw.strip()
        if not raw:
            continue
        try:
            weights.append(int(raw))
        except ValueError:
            return None
    return weights

def all_bad_nodes_below_threshold(weights, bad_node_count, eligible_min_weight,
                                  bad_node_ids=None):
    if weights is None:
        return False
    if bad_node_ids:
        return all(1 <= node_id <= len(weights) and
                   weights[node_id - 1] < eligible_min_weight
                   for node_id in bad_node_ids)
    if bad_node_count is None or bad_node_count <= 0:
        return False
    if len(weights) < bad_node_count:
        return False
    return all(weight < eligible_min_weight for weight in weights[:bad_node_count])

def parse_bad_node_ids(raw):
    if raw is None or raw.strip() == "":
        return []
    ids = []
    for item in raw.split(','):
        item = item.strip()
        if not item:
            continue
        try:
            node_id = int(item)
        except ValueError:
            return []
        if node_id <= 0:
            return []
        ids.append(node_id)
    return ids

def int_from_env(name, default=None):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default

def bad_node_count_from_env_or_config():
    explicit = int_from_env("TD_HS_BAD_NODE_COUNT")
    if explicit is not None:
        return explicit
    explicit = int_from_env("NETWORK_DELAY_NUM")
    if explicit is not None:
        return explicit
    best = 0
    for path in ("config/td_hotstuff.config", "config/template_active.config"):
        try:
            with open(path) as config_file:
                data = json.load(config_file)
        except (OSError, json.JSONDecodeError):
            continue
        try:
            best = max(best, int(data.get("network_delay_num", 0)))
        except (TypeError, ValueError):
            continue
    return best

def read_tps(file, bad_node_count=None, eligible_min_weight=10,
             bad_node_ids=None):
    parsed = ParsedLog()
    threshold_reached = False
    with open(file) as f:
        for l in f.readlines():
            weights = parse_active_weights(l)
            if all_bad_nodes_below_threshold(
                    weights, bad_node_count, eligible_min_weight,
                    bad_node_ids=bad_node_ids):
                threshold_reached = True
                parsed.threshold_seen = True
            s = l.split()
            for r in s:
                try:
                  if(r.split(':')[0] == 'txn'):
                      value = int(r.split(':')[1])
                      parsed.tps.append(value)
                      if threshold_reached:
                          parsed.after_threshold_tps.append(value)
                      else:
                          parsed.before_threshold_tps.append(value)
                except:
                  print("s:",s)
            if l.find("req client latency") > 0:
                print("get lat:",s)
                value = parse_positive_float(s[-1].split(':')[-1])
                if value is not None:
                    parsed.lat.append(value)
            if l.find("consensus latency :") > 0 and l.find("consensus latency :-nan") == -1:
                print("get consensus latency:",s[-10])
                value = parse_positive_float(s[-7].split(':')[-1])
                if value is not None:
                    parsed.lat2.append(value)
            if l.find("propose latency :") > 0 and l.find("propose latency :-nan") == -1:
                print("get propose latency:",s[-7])
                value = parse_positive_float(s[-4].split(':')[-1])
                if value is not None:
                    parsed.lat3.append(value)
            if l.find("reply latency:") > 0:
                print("get reply latency:",s[-1])
                value = parse_positive_float(s[-1].split(':')[-1])
                if value is not None:
                    parsed.lat4.append(value)

                 
    return parsed

def cal_tps(tps, tot, label=""):
    tps_sum = []
    tps_max = 0
    prefix = f"{label} " if label else ""

    for v in tps:
        if v <= 0:
            continue
        tps_max = max(tps_max, v)
        tps_sum.append(v) 

    tps_sum.sort()
    trimmed_tps = tps_sum[tot:] if tot > 0 else tps_sum
    if len(trimmed_tps) == 0 and len(tps_sum) > 0:
        # Tail-forking can produce only one positive sample per replica. In
        # that case, dropping one warmup sample per log deletes all evidence
        # and turns a low-throughput run into a parser crash.
        trimmed_tps = tps_sum
    print(prefix + "tsp:", trimmed_tps)
    if len(trimmed_tps) == 0:
        print(prefix + "average throughput:", 0)
        return tps_max, 0
    # print("max throughput:",tps_max)
    avg_tps = sum(trimmed_tps) / len(trimmed_tps)
    print(prefix + "average throughput:", avg_tps)
    return tps_max, avg_tps

def cal_lat(lat, tot):
    lat_sum = []
    lat_max = 0
    for v in lat:
        if not valid_positive_number(v):
            continue
        lat_max = max(lat_max, v)
        lat_sum.append(v) 

    # tot = int(tot)
    # lat_sum = lat_sum[:-tot]
    # print("max latency:",lat_max)
    if len(lat_sum) == 0:
        print("average latency:", 0)
        return 0, 0
    print("average latency:",sum(lat_sum)/len(lat_sum))
    return lat_max, sum(lat_sum)/len(lat_sum)

def cal_lat2(lat, tot):
    lat_sum = []
    lat_max = 0
    for v in lat:
        if not valid_positive_number(v):
            continue
        lat_max = max(lat_max, v)
        lat_sum.append(v) 

    tot = int(tot)
    # lat_sum = lat_sum[:-tot]
    if len(lat_sum) == 0:
        print("max consensus latency:", 0)
        print("average consensus latency:", 0)
        return 0, 0
    print("max consensus latency:",lat_max)
    print("average consensus latency:",sum(lat_sum)/len(lat_sum))
    return lat_max, sum(lat_sum)/len(lat_sum)

def cal_lat3(lat, tot):
    if len(lat) == 0: 
        return 0, 0
    lat_sum = []
    lat_max = 0
    for v in lat:
        if not valid_positive_number(v):
            continue
        lat_max = max(lat_max, v)
        lat_sum.append(v) 

    tot = int(tot)
    # lat_sum = lat_sum[:-tot]
    if len(lat_sum) == 0:
        print("max propose latency:", 0)
        print("average propose latency:", 0)
        return 0, 0
    print("max propose latency:",lat_max)
    print("average propose latency:",sum(lat_sum)/len(lat_sum))
    return lat_max, sum(lat_sum)/len(lat_sum)


def cal_lat4(lat, tot):
    if len(lat) == 0: 
        return 0, 0
    lat_sum = []
    lat_max = 0
    for v in lat:
        if not valid_positive_number(v):
            continue
        lat_max = max(lat_max, v)
        lat_sum.append(v) 

    tot = int(tot)
    # lat_sum = lat_sum[:-tot]
    if len(lat_sum) == 0:
        print("max reply latency:", 0)
        print("average reply latency:", 0)
        return 0, 0
    print("max reply latency:",lat_max)
    print("average reply latency:",sum(lat_sum)/len(lat_sum))
    return lat_max, sum(lat_sum)/len(lat_sum)

if __name__ == '__main__':
    files = sys.argv[1:]
    print("calculate results, number of nodes:",len(files))


    tps = []
    lat = []
    lat2 = []
    lat3 = []
    lat4 = []
    before_threshold_tps = []
    after_threshold_tps = []
    threshold_seen = False
    bad_node_count = bad_node_count_from_env_or_config()
    bad_node_ids = parse_bad_node_ids(os.environ.get("TD_HS_BAD_NODE_IDS"))
    eligible_min_weight = int_from_env("TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT", 10)
    total = len(files)
    for f in files:
        parsed = read_tps(f, bad_node_count=bad_node_count,
                          eligible_min_weight=eligible_min_weight,
                          bad_node_ids=bad_node_ids)
        t, l, l2, l3, l4=parsed
        # print(t)
        tps += t
        lat += l
        lat2 += l2
        lat3 += l3
        lat4 += l4
        before_threshold_tps += parsed.before_threshold_tps
        after_threshold_tps += parsed.after_threshold_tps
        threshold_seen = threshold_seen or parsed.threshold_seen

    max_tps, avg_tps = cal_tps(tps, len(files))
    if bad_node_count > 0 or bad_node_ids:
        print("bad-node threshold split: bad_node_count:",
              bad_node_count, "eligible_min_weight:", eligible_min_weight,
              "bad_node_ids:", ",".join(str(v) for v in bad_node_ids),
              "found:", int(threshold_seen))
        cal_tps(before_threshold_tps, 0, "before bad-node threshold")
        cal_tps(after_threshold_tps, 0, "after bad-node threshold")
    max_lat, avg_lat = cal_lat(lat, len(files)/2)
    print(avg_tps)
    print(avg_lat)
    # max_lat3, avg_lat3 = cal_lat3(lat3, len(files)/2)
    # max_lat2, avg_lat2 = cal_lat2(lat2, len(files)/2)
    # max_lat4, avg_lat4 = cal_lat4(lat4, len(files)/2)
