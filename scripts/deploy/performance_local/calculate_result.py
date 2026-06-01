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

WEIGHT_RE = re.compile(r"active_weights:\[([^\]]*)\]")

class ParsedLog:
    def __init__(self):
        self.tps = []
        self.lat = []
        self.before_threshold_tps = []
        self.after_threshold_tps = []
        self.threshold_seen = False

    def __iter__(self):
        yield self.tps
        yield self.lat

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
    try:
        with open("config/td_hotstuff.config") as config_file:
            return int(json.load(config_file).get("network_delay_num", 0))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return 0

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
                if(r.split(':')[0] == 'txn'):
                    value = int(r.split(':')[1])
                    parsed.tps.append(value)
                    if threshold_reached:
                        parsed.after_threshold_tps.append(value)
                    else:
                        parsed.before_threshold_tps.append(value)
            if l.find("client latency") > 0:
                value = parse_positive_float(s[-1].split(':')[-1])
                if value is not None:
                    parsed.lat.append(value)
    return parsed

def cal_tps(tps, label=""):
    tps_sum = []
    tps_max = 0
    prefix = f"{label} " if label else ""

    for v in tps:
        if v == 0:
            continue
        tps_max = max(tps_max, v)
        tps_sum.append(v) 

    print(prefix + "max throughput:",tps_max)
    if len(tps_sum) == 0:
        print(prefix + "average throughput:", 0)
        return
    print(prefix + "average throughput:",sum(tps_sum)/len(tps_sum))

def cal_lat(lat):
    lat_sum = []
    lat_max = 0
    for v in lat:
        if not valid_positive_number(v):
            continue
        lat_max = max(lat_max, v)
        lat_sum.append(v) 

    print("max latency:",lat_max)
    if len(lat_sum) == 0:
        print("average latency:", 0)
        return
    print("average latency:",sum(lat_sum)/len(lat_sum))

if __name__ == '__main__':
    files = sys.argv[1:]
    print("calculate results, number of nodes:",len(files))


    tps = []
    lat = []
    before_threshold_tps = []
    after_threshold_tps = []
    threshold_seen = False
    bad_node_count = bad_node_count_from_env_or_config()
    bad_node_ids = parse_bad_node_ids(os.environ.get("TD_HS_BAD_NODE_IDS"))
    eligible_min_weight = int_from_env("TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT", 10)
    for f in files:
        parsed=read_tps(f, bad_node_count=bad_node_count,
                        eligible_min_weight=eligible_min_weight,
                        bad_node_ids=bad_node_ids)
        t, l=parsed
        tps += t
        lat += l
        before_threshold_tps += parsed.before_threshold_tps
        after_threshold_tps += parsed.after_threshold_tps
        threshold_seen = threshold_seen or parsed.threshold_seen

    cal_tps(tps)
    if bad_node_count > 0 or bad_node_ids:
        print("bad-node threshold split: bad_node_count:",
              bad_node_count, "eligible_min_weight:", eligible_min_weight,
              "bad_node_ids:", ",".join(str(v) for v in bad_node_ids),
              "found:", int(threshold_seen))
        cal_tps(before_threshold_tps, "before bad-node threshold")
        cal_tps(after_threshold_tps, "after bad-node threshold")
    cal_lat(lat)
