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

import datetime
import json
import math
import os
import re
import sys

WEIGHT_RE = re.compile(r"active_weights:\[([^\]]*)\]")
LEADER_WEIGHT_RE = re.compile(r"leader_weights:\[([^\]]*)\]")
GLOG_TIME_RE = re.compile(
    r"^[A-Z](\d{8})\s+(\d{2}:\d{2}:\d{2})(?:\.(\d+))?")
TXN_RE = re.compile(r"(?:^|\s)txn:(\d+)(?:\s|$)")
BENCH_TIME_RE = re.compile(
    r"(?:^|\s)time:([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?)(?:\s|$)")
LATENCY_RE = re.compile(r"%s\s*:([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?)")

MAX_LATENCY_SECONDS = float(os.environ.get("TD_HS_RESULT_MAX_LATENCY_SECONDS", "300"))
STEADY_AFTER_THRESHOLD_GRACE_SECONDS = float(
    os.environ.get("TD_HS_RESULT_STEADY_GRACE_SECONDS", "10"))
RESULT_WARMUP_SECONDS = float(os.environ.get("TD_HS_RESULT_WARMUP_SECONDS", "30"))
RESULT_WARMUP_SAMPLE_RATIO = float(
    os.environ.get("TD_HS_RESULT_WARMUP_SAMPLE_RATIO", "0.20"))
RESULT_COOLDOWN_SECONDS = float(os.environ.get("TD_HS_RESULT_COOLDOWN_SECONDS", "10"))
RESULT_COOLDOWN_SAMPLE_RATIO = float(
    os.environ.get("TD_HS_RESULT_COOLDOWN_SAMPLE_RATIO", "0.05"))

class ParsedLog:
    def __init__(self):
        self.tps = []
        self.lat = []
        self.lat2 = []
        self.lat3 = []
        self.lat4 = []
        self.warmup_tps = []
        self.stable_tps = []
        self.warmup_lat = []
        self.stable_lat = []
        self.stable_window_fallback = False
        self.before_threshold_tps = []
        self.after_threshold_tps = []
        self.before_threshold_lat = []
        self.after_threshold_lat = []
        self.steady_after_threshold_tps = []
        self.steady_after_threshold_lat = []
        self.threshold_seen = False
        self.final_weights = None
        self.final_leader_weights = None

    def __iter__(self):
        yield self.tps
        yield self.lat
        yield self.lat2
        yield self.lat3
        yield self.lat4

def valid_positive_number(value):
    return math.isfinite(value) and value > 0

def parse_positive_float(raw, max_value=None):
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not valid_positive_number(value):
        return None
    if max_value is not None and value > max_value:
        return None
    return value

def parse_timestamp(line):
    match = GLOG_TIME_RE.match(line)
    if not match:
        return None
    date_part, time_part, micros = match.groups()
    micros = (micros or "0")[:6].ljust(6, "0")
    try:
        dt = datetime.datetime.strptime(
            f"{date_part} {time_part}.{micros}", "%Y%m%d %H:%M:%S.%f")
    except ValueError:
        return None
    return dt.timestamp()

def parse_benchmark_time(line):
    match = BENCH_TIME_RE.search(line)
    if not match:
        return None
    return parse_positive_float(match.group(1))

def parse_weights_from_match(match):
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

def parse_active_weights(line):
    return parse_weights_from_match(WEIGHT_RE.search(line))

def parse_leader_weights(line):
    return parse_weights_from_match(LEADER_WEIGHT_RE.search(line))

def reputation_file_for_log(log_file):
    base = os.path.basename(log_file)
    match = re.match(r"result_(\d+)_log$", base)
    if not match:
        return None
    return os.path.join(os.path.dirname(log_file),
                        f"result_{match.group(1)}_reputation.jsonl")

def read_reputation_summary(log_files):
    best_record = None
    best_key = (-1, -1)
    max_fault_counts = []
    max_penalty_points = []
    min_sybil_graph_scores = []
    max_sybil_graph_debt = []
    for log_file in log_files:
        reputation_file = reputation_file_for_log(log_file)
        if not reputation_file or not os.path.exists(reputation_file):
            continue
        try:
            lines = open(reputation_file)
        except OSError:
            continue
        with lines:
            for line in lines:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                validators = record.get("validators", [])
                if validators:
                    if len(max_fault_counts) < len(validators):
                        max_fault_counts.extend([0] * (len(validators) - len(max_fault_counts)))
                        max_penalty_points.extend([0] * (len(validators) - len(max_penalty_points)))
                        min_sybil_graph_scores.extend([100] * (len(validators) - len(min_sybil_graph_scores)))
                        max_sybil_graph_debt.extend([0] * (len(validators) - len(max_sybil_graph_debt)))
                    for validator in validators:
                        try:
                            idx = int(validator.get("validator_id", 0)) - 1
                        except (TypeError, ValueError):
                            continue
                        if 0 <= idx < len(max_fault_counts):
                            max_fault_counts[idx] = max(
                                max_fault_counts[idx],
                                int(validator.get("strong_fault_count", 0)))
                            max_penalty_points[idx] = max(
                                max_penalty_points[idx],
                                int(validator.get("penalty_points", 0)))
                            if "sybil_graph_score" in validator:
                                min_sybil_graph_scores[idx] = min(
                                    min_sybil_graph_scores[idx],
                                    int(validator.get("sybil_graph_score", 100)))
                            if "sybil_graph_debt" in validator:
                                max_sybil_graph_debt[idx] = max(
                                    max_sybil_graph_debt[idx],
                                    int(validator.get("sybil_graph_debt", 0)))
                try:
                    key = (int(record.get("old_weight_version", 0)),
                           int(record.get("window_index", 0)))
                except (TypeError, ValueError):
                    key = (0, 0)
                if key >= best_key:
                    best_key = key
                    best_record = record
    return (best_record, max_fault_counts, max_penalty_points,
            min_sybil_graph_scores, max_sybil_graph_debt)

def print_reputation_summary(log_files, bad_node_ids):
    (record, fault_counts, penalty_points, min_sybil_scores,
     max_sybil_debt) = read_reputation_summary(log_files)
    if record is None:
        return
    print("reputation candidate digest:", record.get("candidate_digest", ""))
    print("strong fault root:", record.get("strong_fault_root", ""))
    print("penalty root:", record.get("penalty_root", ""))
    if record.get("next_weights") is not None:
        print("reputation next weights:",
              ",".join(str(v) for v in record.get("next_weights", [])))
    validators = record.get("validators", [])
    if validators and any("sybil_graph_score" in v for v in validators):
        scores = []
        debts = []
        for validator in validators:
            scores.append(int(validator.get("sybil_graph_score", 100)))
            debts.append(int(validator.get("sybil_graph_debt", 0)))
        print("validator sybil graph scores:",
              ",".join(str(v) for v in scores))
        print("validator sybil graph debt:",
              ",".join(str(v) for v in debts))
        if bad_node_ids:
            bad_scores = [scores[i - 1] for i in bad_node_ids
                          if 1 <= i <= len(scores)]
            bad_debt = [debts[i - 1] for i in bad_node_ids
                        if 1 <= i <= len(debts)]
            honest_scores = [scores[i - 1]
                             for i in range(1, len(scores) + 1)
                             if i not in bad_node_ids]
            honest_debt = [debts[i - 1]
                           for i in range(1, len(debts) + 1)
                           if i not in bad_node_ids]
            print("bad node sybil graph scores:",
                  ",".join(str(v) for v in bad_scores))
            print("bad node sybil graph debt:",
                  ",".join(str(v) for v in bad_debt))
            if honest_scores:
                print("honest sybil graph score min/avg/max:",
                      f"{min(honest_scores)}/{sum(honest_scores)/len(honest_scores):.2f}/{max(honest_scores)}")
            if honest_debt:
                print("honest sybil graph debt min/avg/max:",
                      f"{min(honest_debt)}/{sum(honest_debt)/len(honest_debt):.2f}/{max(honest_debt)}")
    if min_sybil_scores:
        print("observed min sybil graph scores:",
              ",".join(str(v) for v in min_sybil_scores))
        print("observed max sybil graph debt:",
              ",".join(str(v) for v in max_sybil_debt))
        if bad_node_ids:
            bad_min_scores = [min_sybil_scores[i - 1] for i in bad_node_ids
                              if 1 <= i <= len(min_sybil_scores)]
            bad_max_debt = [max_sybil_debt[i - 1] for i in bad_node_ids
                            if 1 <= i <= len(max_sybil_debt)]
            honest_min_scores = [min_sybil_scores[i - 1]
                                 for i in range(1, len(min_sybil_scores) + 1)
                                 if i not in bad_node_ids]
            honest_max_debt = [max_sybil_debt[i - 1]
                               for i in range(1, len(max_sybil_debt) + 1)
                               if i not in bad_node_ids]
            print("bad node observed min sybil graph scores:",
                  ",".join(str(v) for v in bad_min_scores))
            print("bad node observed max sybil graph debt:",
                  ",".join(str(v) for v in bad_max_debt))
            if honest_min_scores:
                print("honest observed min sybil graph score min/avg/max:",
                      f"{min(honest_min_scores)}/{sum(honest_min_scores)/len(honest_min_scores):.2f}/{max(honest_min_scores)}")
            if honest_max_debt:
                print("honest observed max sybil graph debt min/avg/max:",
                      f"{min(honest_max_debt)}/{sum(honest_max_debt)/len(honest_max_debt):.2f}/{max(honest_max_debt)}")
    if fault_counts:
        print("validator strong fault counts:",
              ",".join(str(v) for v in fault_counts))
        print("validator penalty points:",
              ",".join(str(v) for v in penalty_points))
        if bad_node_ids:
            bad_faults = [fault_counts[i - 1] for i in bad_node_ids
                          if 1 <= i <= len(fault_counts)]
            bad_penalties = [penalty_points[i - 1] for i in bad_node_ids
                             if 1 <= i <= len(penalty_points)]
            print("bad node strong fault counts:",
                  ",".join(str(v) for v in bad_faults))
            print("bad node penalty points:",
                  ",".join(str(v) for v in bad_penalties))

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
    for cfg_path in ("config/td_hotstuff.config", "config/template_active.config"):
        try:
            with open(cfg_path) as config_file:
                data = json.load(config_file)
        except (OSError, json.JSONDecodeError):
            continue
        try:
            best = max(best, int(data.get("network_delay_num", 0)))
        except (TypeError, ValueError):
            continue
    return best

def threshold_weights_for_line(line):
    leader_weights = parse_leader_weights(line)
    if leader_weights is not None:
        return leader_weights
    return parse_active_weights(line)

def find_global_threshold_time(files, bad_node_count, eligible_min_weight,
                               bad_node_ids=None):
    threshold_time = None
    for file in files:
        try:
            fh = open(file)
        except OSError:
            continue
        with fh:
            for line in fh:
                weights = threshold_weights_for_line(line)
                if not all_bad_nodes_below_threshold(
                        weights, bad_node_count, eligible_min_weight,
                        bad_node_ids=bad_node_ids):
                    continue
                line_time = parse_timestamp(line)
                if line_time is None:
                    continue
                if threshold_time is None or line_time < threshold_time:
                    threshold_time = line_time
    return threshold_time

def classify_window(line_time, threshold_time):
    if threshold_time is None or line_time is None:
        return "before"
    return "after" if line_time >= threshold_time else "before"

def is_steady_after_threshold(line_time, threshold_time):
    return (threshold_time is not None and line_time is not None and
            line_time >= threshold_time + STEADY_AFTER_THRESHOLD_GRACE_SECONDS)

def parse_line_latency(line, label):
    match = LATENCY_RE.pattern % re.escape(label)
    found = re.search(match, line)
    if not found:
        return None
    return parse_positive_float(found.group(1), MAX_LATENCY_SECONDS)

def bounded_warmup_ratio():
    if not math.isfinite(RESULT_WARMUP_SAMPLE_RATIO):
        return 0.20
    return max(0.0, min(0.95, RESULT_WARMUP_SAMPLE_RATIO))

def bounded_cooldown_ratio():
    if not math.isfinite(RESULT_COOLDOWN_SAMPLE_RATIO):
        return 0.05
    return max(0.0, min(0.95, RESULT_COOLDOWN_SAMPLE_RATIO))

def split_stable_records(records, first_txn_time):
    if not records:
        return [], [], False
    has_usable_time = first_txn_time is not None and any(
        line_time is not None for _, line_time in records)
    if has_usable_time:
        stable_cutoff = first_txn_time + max(0.0, RESULT_WARMUP_SECONDS)
        timed_records = [(value, line_time) for value, line_time in records
                         if line_time is not None]
        last_record_time = max(line_time for _, line_time in timed_records)
        cooldown_cutoff = last_record_time - max(0.0, RESULT_COOLDOWN_SECONDS)
        warmup = [value for value, line_time in records
                  if line_time is None or line_time < stable_cutoff]
        stable = [value for value, line_time in timed_records
                  if line_time >= stable_cutoff]
        if len(stable) >= 10 and cooldown_cutoff > stable_cutoff:
            cooled_stable = [value for value, line_time in timed_records
                             if (line_time >= stable_cutoff and
                                 line_time < cooldown_cutoff)]
            if cooled_stable:
                stable = cooled_stable
    else:
        warmup_count = int(math.ceil(len(records) * bounded_warmup_ratio()))
        warmup_count = max(0, min(len(records) - 1, warmup_count))
        cooldown_count = int(math.ceil(len(records) * bounded_cooldown_ratio()))
        cooldown_count = max(0, min(len(records) - warmup_count - 1,
                                    cooldown_count))
        stable_end = len(records) - cooldown_count
        warmup = [value for value, _ in records[:warmup_count]]
        stable = [value for value, _ in records[warmup_count:stable_end]]
    if stable:
        return warmup, stable, False
    return warmup, [value for value, _ in records], True

def read_tps(file, threshold_time=None, bad_node_count=None,
             eligible_min_weight=11, bad_node_ids=None):
    parsed = ParsedLog()
    last_timestamp = None
    first_txn_time = None
    txn_records = []
    latency_records = []
    local_threshold_active = False
    with open(file) as f:
        for line in f:
            parsed_time = parse_timestamp(line)
            if parsed_time is not None:
                last_timestamp = parsed_time
            benchmark_time = parse_benchmark_time(line)
            line_time = parsed_time if parsed_time is not None else benchmark_time
            if line_time is None:
                line_time = last_timestamp
            weights = parse_active_weights(line)
            if weights is not None:
                parsed.final_weights = weights
            leader_weights = parse_leader_weights(line)
            if leader_weights is not None:
                parsed.final_leader_weights = leader_weights
            threshold_reached_on_line = all_bad_nodes_below_threshold(
                    threshold_weights_for_line(line), bad_node_count,
                    eligible_min_weight, bad_node_ids=bad_node_ids)
            if threshold_reached_on_line:
                parsed.threshold_seen = True
                if threshold_time is None:
                    local_threshold_active = True
            window = classify_window(line_time, threshold_time)
            if threshold_time is None and local_threshold_active:
                window = "after"
            steady = is_steady_after_threshold(line_time, threshold_time)

            for match in TXN_RE.finditer(line):
                value = int(match.group(1))
                parsed.tps.append(value)
                txn_records.append((value, line_time))
                if first_txn_time is None and line_time is not None:
                    first_txn_time = line_time
                if window == "after":
                    parsed.after_threshold_tps.append(value)
                    if steady:
                        parsed.steady_after_threshold_tps.append(value)
                else:
                    parsed.before_threshold_tps.append(value)

            value = parse_line_latency(line, "req client latency")
            if value is not None:
                parsed.lat.append(value)
                latency_records.append((value, line_time))
                if window == "after":
                    parsed.after_threshold_lat.append(value)
                    if steady:
                        parsed.steady_after_threshold_lat.append(value)
                else:
                    parsed.before_threshold_lat.append(value)

            value = parse_line_latency(line, "consensus latency")
            if value is not None:
                parsed.lat2.append(value)
            value = parse_line_latency(line, "propose latency")
            if value is not None:
                parsed.lat3.append(value)
            value = parse_line_latency(line, "reply latency")
            if value is not None:
                parsed.lat4.append(value)
    (parsed.warmup_tps, parsed.stable_tps,
     tps_fallback) = split_stable_records(txn_records, first_txn_time)
    (parsed.warmup_lat, parsed.stable_lat,
     lat_fallback) = split_stable_records(latency_records, first_txn_time)
    parsed.stable_window_fallback = tps_fallback or lat_fallback
    return parsed

def percentile(sorted_values, pct):
    if not sorted_values:
        return 0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (len(sorted_values) - 1) * pct / 100.0
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return sorted_values[lower]
    fraction = rank - lower
    return sorted_values[lower] * (1.0 - fraction) + sorted_values[upper] * fraction

def cal_tps(tps, tot, label=""):
    tps_sum = []
    tps_max = 0
    prefix = f"{label} " if label else ""
    for value in tps:
        if value <= 0:
            continue
        tps_max = max(tps_max, value)
        tps_sum.append(value)
    tps_sum.sort()
    trimmed_tps = tps_sum[tot:] if tot > 0 else tps_sum
    if len(trimmed_tps) == 0 and len(tps_sum) > 0:
        trimmed_tps = tps_sum
    print(prefix + "tsp:", trimmed_tps)
    if len(trimmed_tps) == 0:
        print(prefix + "average throughput:", 0)
        return tps_max, 0
    avg_tps = sum(trimmed_tps) / len(trimmed_tps)
    print(prefix + "average throughput:", avg_tps)
    return tps_max, avg_tps

def cal_lat(lat, tot, label=""):
    lat_sum = []
    lat_max = 0
    for value in lat:
        if not valid_positive_number(value):
            continue
        lat_max = max(lat_max, value)
        lat_sum.append(value)
    prefix = f"{label} " if label else ""
    if len(lat_sum) == 0:
        print(prefix + "latency samples:", 0)
        print(prefix + "average latency:", 0)
        return 0, 0
    lat_sum.sort()
    print(prefix + "latency samples:", len(lat_sum))
    print(prefix + "max latency:", lat_max)
    print(prefix + "p50 latency:", percentile(lat_sum, 50))
    print(prefix + "p95 latency:", percentile(lat_sum, 95))
    avg = sum(lat_sum) / len(lat_sum)
    print(prefix + "average latency:", avg)
    return lat_max, avg

def cal_lat2(lat, tot):
    return cal_named_latency(lat, "consensus")

def cal_lat3(lat, tot):
    return cal_named_latency(lat, "propose")

def cal_lat4(lat, tot):
    return cal_named_latency(lat, "reply")

def cal_named_latency(lat, name):
    lat_sum = [v for v in lat if valid_positive_number(v)]
    if not lat_sum:
        print(f"{name} latency samples:", 0)
        print(f"max {name} latency:", 0)
        print(f"average {name} latency:", 0)
        return 0, 0
    lat_sum.sort()
    print(f"{name} latency samples:", len(lat_sum))
    print(f"max {name} latency:", max(lat_sum))
    print(f"p50 {name} latency:", percentile(lat_sum, 50))
    print(f"p95 {name} latency:", percentile(lat_sum, 95))
    avg = sum(lat_sum) / len(lat_sum)
    print(f"average {name} latency:", avg)
    return max(lat_sum), avg

def print_final_weight_summary(final_weights_by_log, final_leader_weights_by_log,
                               bad_node_count, bad_node_ids):
    if not final_weights_by_log:
        return
    final_weights = final_weights_by_log[-1]
    has_bad_nodes = bool(bad_node_ids) or bad_node_count > 0
    if bad_node_ids:
        bad_indexes = [node_id - 1 for node_id in bad_node_ids
                       if 1 <= node_id <= len(final_weights)]
    elif has_bad_nodes:
        bad_indexes = list(range(min(bad_node_count, len(final_weights))))
    else:
        bad_indexes = []
    bad_index_set = set(bad_indexes)
    bad_weights = [final_weights[i] for i in bad_indexes]
    honest_weights = [weight for i, weight in enumerate(final_weights)
                      if i not in bad_index_set]
    print("final active weights:", ",".join(str(v) for v in final_weights))
    if has_bad_nodes:
        print("bad node final weights:", ",".join(str(v) for v in bad_weights))
    if final_leader_weights_by_log:
        final_leader_weights = final_leader_weights_by_log[-1]
        print("final active leader weights:",
              ",".join(str(v) for v in final_leader_weights))
        if has_bad_nodes:
            leader_bad_weights = [final_leader_weights[i] for i in bad_indexes
                                  if i < len(final_leader_weights)]
            print("bad node final leader weights:",
                  ",".join(str(v) for v in leader_bad_weights))
    if honest_weights:
        honest_avg = sum(honest_weights) / len(honest_weights)
        print("honest final weights min/avg/max:", min(honest_weights),
              honest_avg, max(honest_weights))

if __name__ == '__main__':
    files = sys.argv[1:]
    print("calculate results, number of nodes:", len(files))

    tps = []
    lat = []
    lat2 = []
    lat3 = []
    lat4 = []
    warmup_tps = []
    stable_tps = []
    warmup_lat = []
    stable_lat = []
    stable_window_fallback = False
    before_threshold_tps = []
    after_threshold_tps = []
    steady_after_threshold_tps = []
    before_threshold_lat = []
    after_threshold_lat = []
    steady_after_threshold_lat = []
    final_weights_by_log = []
    final_leader_weights_by_log = []
    threshold_seen = False
    bad_node_count = bad_node_count_from_env_or_config()
    bad_node_ids = parse_bad_node_ids(os.environ.get("TD_HS_BAD_NODE_IDS"))
    eligible_min_weight = int_from_env("TD_HS_LEADER_ELIGIBLE_MIN_WEIGHT", 11)
    threshold_time = None
    if bad_node_count > 0 or bad_node_ids:
        threshold_time = find_global_threshold_time(
            files, bad_node_count, eligible_min_weight,
            bad_node_ids=bad_node_ids)

    for file in files:
        parsed = read_tps(file, threshold_time=threshold_time,
                          bad_node_count=bad_node_count,
                          eligible_min_weight=eligible_min_weight,
                          bad_node_ids=bad_node_ids)
        parsed_tps, parsed_lat, parsed_lat2, parsed_lat3, parsed_lat4 = parsed
        tps += parsed_tps
        lat += parsed_lat
        lat2 += parsed_lat2
        lat3 += parsed_lat3
        lat4 += parsed_lat4
        warmup_tps += parsed.warmup_tps
        stable_tps += parsed.stable_tps
        warmup_lat += parsed.warmup_lat
        stable_lat += parsed.stable_lat
        stable_window_fallback = (
            stable_window_fallback or parsed.stable_window_fallback)
        before_threshold_tps += parsed.before_threshold_tps
        after_threshold_tps += parsed.after_threshold_tps
        steady_after_threshold_tps += parsed.steady_after_threshold_tps
        before_threshold_lat += parsed.before_threshold_lat
        after_threshold_lat += parsed.after_threshold_lat
        steady_after_threshold_lat += parsed.steady_after_threshold_lat
        if parsed.final_weights is not None:
            final_weights_by_log.append(parsed.final_weights)
        if parsed.final_leader_weights is not None:
            final_leader_weights_by_log.append(parsed.final_leader_weights)
        threshold_seen = threshold_seen or parsed.threshold_seen

    max_raw_tps, avg_raw_tps = cal_tps(tps, len(files), "raw")
    print("warmup throughput samples:", len(warmup_tps))
    print("stable throughput samples:", len(stable_tps))
    if stable_window_fallback:
        print("stable window fallback: insufficient samples after warmup")
    max_tps, avg_tps = cal_tps(stable_tps, 0, "stable")
    if bad_node_count > 0 or bad_node_ids:
        print("bad-node threshold split: bad_node_count:",
              bad_node_count, "eligible_min_weight:", eligible_min_weight,
              "bad_node_ids:", ",".join(str(v) for v in bad_node_ids),
              "found:", int(threshold_time is not None or threshold_seen),
              "threshold_time:", threshold_time if threshold_time is not None else "n/a")
        cal_tps(before_threshold_tps, 0, "before bad-node threshold")
        cal_tps(after_threshold_tps, 0, "after bad-node threshold")
        cal_tps(steady_after_threshold_tps, 0,
                "steady after bad-node threshold")
        cal_lat(before_threshold_lat, 0, "before bad-node threshold")
        cal_lat(after_threshold_lat, 0, "after bad-node threshold")
        cal_lat(steady_after_threshold_lat, 0,
                "steady after bad-node threshold")
        print_final_weight_summary(final_weights_by_log,
                                   final_leader_weights_by_log,
                                   bad_node_count, bad_node_ids)
        print_reputation_summary(files, bad_node_ids)
    else:
        print_final_weight_summary(final_weights_by_log,
                                   final_leader_weights_by_log,
                                   bad_node_count, bad_node_ids)
    max_raw_lat, avg_raw_lat = cal_lat(lat, len(files) / 2, "raw")
    print("warmup latency samples:", len(warmup_lat))
    max_lat, avg_lat = cal_lat(stable_lat, 0, "stable")
    cal_lat4(lat4, len(files) / 2)
    print(avg_tps)
    print(avg_lat)
