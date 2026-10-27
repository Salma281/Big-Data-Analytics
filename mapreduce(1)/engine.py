"""A small MapReduce-like engine. No Hadoop, no Spark, no Counter, no groupby:
the map / combine / partition / shuffle / sort / reduce code is all written here."""
import os, re, time
from concurrent.futures import ProcessPoolExecutor

NUM_REDUCERS = 2
LINE_RE = re.compile(r"^\s*(.+?)[\s,;:=]+(-?\d+(?:\.\d+)?)\s*$")
MODES = ["wordcount", "top10", "sum", "count", "min", "max", "average"]


def partition(key):
    """Reducer 0 gets words a-f, reducer 1 gets g-z (and digits/other)."""
    c = key[:1].lower()
    return 0 if "a" <= c <= "f" else 1


def merge(mode, a, b):
    """Combine two partial values of the same key (also used by reduce)."""
    if mode == "min":
        return min(a, b)
    if mode == "max":
        return max(a, b)
    if mode == "average":                       # values are (sum, count) pairs
        return (a[0] + b[0], a[1] + b[1])
    return a + b                                # wordcount, sum, count


def finalize(mode, v):
    return v[0] / v[1] if mode == "average" else v


def top_k(pairs, k=10):
    """Return the k highest-frequency keys, breaking ties alphabetically."""
    return sorted(pairs, key=lambda pair: (-pair[1], pair[0]))[:k]


# ---------- MAP TASK: map -> combine -> partition ----------
def map_phase(text, mode):
    pairs, skipped = [], 0
    if mode in ("wordcount", "top10"):
        for w in re.findall(r"[a-z0-9']+", text.lower()):
            pairs.append((w, 1))
        return pairs, skipped
    for line in text.splitlines():
        if not line.strip():
            continue
        if mode == "count":
            m = LINE_RE.match(line)
            key = m.group(1) if m else re.split(r"[,;:\t]", line)[0]
            pairs.append((key.strip().lower(), 1))
            continue
        # accepts "Chai,3"  "Chai 3"  "Chai: 3"  "Chai;3.5"  (key, then a number at the end)
        m = LINE_RE.match(line)
        if not m:
            skipped += 1
            continue
        key, v = m.group(1).strip().lower(), float(m.group(2))
        pairs.append((key, (v, 1)) if mode == "average" else (key, v))
    return pairs, skipped


def combine(pairs, mode):
    local = {}
    for k, v in pairs:
        local[k] = merge(mode, local[k], v) if k in local else v
    return list(local.items())


def map_task(args):
    name, text, mode, use_combiner = args
    start = time.time()
    mapped, skipped = map_phase(text, mode)
    combined = combine(mapped, mode) if use_combiner else mapped
    buckets = [[] for _ in range(NUM_REDUCERS)]
    for k, v in combined:
        buckets[partition(k)].append((k, v))
    return dict(split=name, pid=os.getpid(), start=start, end=time.time(),
                preview=text[:160], mapped=mapped, combined=combined,
                skipped=skipped, partitions=buckets)


# ---------- REDUCE TASK: shuffle & sort -> reduce ----------
def sort_group(pairs):
    groups = []
    for k, v in sorted(pairs, key=lambda p: p[0]):
        if groups and groups[-1][0] == k:
            groups[-1][1].append(v)
        else:
            groups.append((k, [v]))
    return groups


def reduce_task(args):
    r, pairs, mode, outdir = args
    start = time.time()
    groups = sort_group(pairs)
    result = []
    for k, vals in groups:
        acc = vals[0]
        for v in vals[1:]:
            acc = merge(mode, acc, v)
        result.append((k, finalize(mode, acc)))
    fname = "part-%05d" % r
    with open(os.path.join(outdir, fname), "w", encoding="utf-8") as f:
        for k, v in result:
            f.write("%s\t%s\n" % (k, ("%g" % v) if isinstance(v, float) else v))
    return dict(reducer=r, pid=os.getpid(), start=start, end=time.time(),
                groups=groups, result=result, file=fname)


def run_job(files, mode="wordcount", use_combiner=True, outdir="output"):
    """files: list of (filename, text). One file = one split = one map task."""
    os.makedirs(outdir, exist_ok=True)
    for old in os.listdir(outdir):
        if old.startswith("part-"):
            os.remove(os.path.join(outdir, old))
    t0 = time.time()
    workers = max(1, min(len(files), os.cpu_count() or 1))
    with ProcessPoolExecutor(max_workers=workers) as pool:
        maps = list(pool.map(map_task, [(n, t, mode, use_combiner) for n, t in files]))
        # barrier: all mappers are done. SHUFFLE: route bucket r of every mapper to reducer r
        received = [[(m["split"], m["partitions"][r]) for m in maps] for r in range(NUM_REDUCERS)]
        inbox = [[p for _, ps in received[r] for p in ps] for r in range(NUM_REDUCERS)]
        reds = list(pool.map(reduce_task, [(r, inbox[r], mode, outdir) for r in range(NUM_REDUCERS)]))
    if mode == "top10":
        selected = {key for key, _ in top_k(
            [pair for reducer in reds for pair in reducer["result"]])}
        for reducer in reds:
            reducer["result"] = sorted(
                (pair for pair in reducer["result"] if pair[0] in selected),
                key=lambda pair: (-pair[1], pair[0]))
            with open(os.path.join(outdir, reducer["file"]), "w", encoding="utf-8") as f:
                for key, value in reducer["result"]:
                    f.write("%s\t%s\n" % (key, ("%g" % value) if isinstance(value, float) else value))
    return dict(t0=t0, t1=time.time(), mode=mode, skipped=sum(m["skipped"] for m in maps), use_combiner=use_combiner,
                workers=workers, maps=maps, received=received, reducers=reds)
