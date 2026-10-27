import os, secrets
from collections import OrderedDict
from flask import Flask, render_template, request, redirect, session, url_for
import engine

app = Flask(__name__)
app.secret_key = secrets.token_hex(16)
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
LABELS = {"wordcount": "Word count", "top10": "Top 10 Most Frequent Words", "sum": "Sum", "count": "Count",
          "min": "Min", "max": "Max", "average": "Average"}
VIEWS = ("simple", "detail", "flow")
SHOWN = 8                     # mappers drawn in the flow map
STORE = OrderedDict()         # per-browser saved inputs and last job (in memory)


def fmt(v):
    if isinstance(v, tuple):
        return "(%s, %s)" % (fmt(v[0]), v[1])
    return ("%g" % v) if isinstance(v, float) else str(v)


def pair(p):
    return "(%s, %s)" % (p[0], fmt(p[1]))


app.add_template_filter(fmt, "fmt")
app.add_template_filter(pair, "pair")


def state():
    sid = session.get("sid")
    if sid not in STORE:
        sid = session["sid"] = secrets.token_hex(8)
        STORE[sid] = {}
        while len(STORE) > 20:
            STORE.popitem(last=False)
    return STORE[sid]


def _items(pairs, cap=150):
    out = [pair(p) for p in pairs[:cap]]
    if len(pairs) > cap:
        out.append("+%d more" % (len(pairs) - cap))
    return out


def flow_info(job):
    maps = job["maps"]
    info = {"fo": {"title": "Input folder", "sections": [
        {"label": "%d files, one split each" % len(maps), "items": [m["split"] for m in maps][:150]}]}}
    for i, m in enumerate(maps[:SHOWN]):
        info["s%d" % i] = {"title": "Split %d · %s" % (i + 1, m["split"]), "sections": [
            {"label": "Start of the file", "items": [m["preview"].replace("\n", " ⏎ ")]}]}
        info["m%d" % i] = {"title": "Map output", "sections": [
            {"label": "%d pairs emitted" % len(m["mapped"]), "items": _items(m["mapped"])}]}
        info["c%d" % i] = {"title": "Combine output", "sections": [
            {"label": "%d → %d pairs (local subtotals)" % (len(m["mapped"]), len(m["combined"])),
             "items": _items(m["combined"])}]}
        info["p%d" % i] = {"title": "Partition", "sections": [
            {"label": "To reducer 1 (a–f)", "items": _items(m["partitions"][0])},
            {"label": "To reducer 2 (g–z)", "items": _items(m["partitions"][1])}]}
    for r in job["reducers"]:
        n = r["reducer"]
        info["sh%d" % n] = {"title": "Shuffle & sort · reducer %d" % (n + 1), "sections": [
            {"label": "Received from " + name, "items": _items(ps)} for name, ps in job["received"][n]]}
        info["rd%d" % n] = {"title": "Reducer %d" % (n + 1), "sections": [
            {"label": "Sorted and grouped (key → values)",
             "items": ["%s → [%s]" % (k, ", ".join(fmt(v) for v in vals)) for k, vals in r["groups"][:150]]},
            {"label": "Reduced result", "items": ["%s = %s" % (k, fmt(v)) for k, v in r["result"][:150]]}]}
        info["o%d" % n] = {"title": r["file"], "sections": [
            {"label": "File contents (key, value)", "items": ["%s, %s" % (k, fmt(v)) for k, v in r["result"][:150]]}]}
    counts = [[len(m["partitions"][0]), len(m["partitions"][1])] for m in maps[:SHOWN]]
    return info, counts


@app.route("/")
def index():
    return render_template("index.html", st=state())


@app.route("/run", methods=["POST"])
def run():
    st = state()
    processing_type = request.form.get("processing_type", "text")
    mode = request.form.get("operation", "wordcount")
    view = request.form.get("view", "simple")
    st["processing_type"], st["operation"] = processing_type, mode
    st["mode"], st["view"] = mode, (view if view in VIEWS else "simple")
    new = []
    for f in request.files.getlist("files"):
        name = os.path.basename(f.filename or "")
        if name.lower().endswith(".txt"):
            new.append((name, f.read().decode("utf-8", errors="replace").lstrip("\ufeff")))
    if new:
        st["files"] = new                       # otherwise reuse the saved folder
    valid_operations = {
        "text": ("wordcount", "top10"),
        "numeric": ("sum", "count", "min", "max", "average"),
    }
    if mode not in valid_operations.get(processing_type, ()) or not st.get("files"):
        return render_template("index.html", st=st,
                               error="Choose a valid operation and a folder that contains at least one .txt file."), 400
    st["job"] = engine.run_job(st["files"], mode, True, OUT)
    return redirect(url_for("result", view=st["view"]))


@app.route("/result")
def result():
    st = state()
    job = st.get("job")
    if not job:
        return redirect(url_for("index"))
    view = request.args.get("view", st.get("view", "simple"))
    if view not in VIEWS:
        view = "simple"
    rows = [(k, v, r["reducer"]) for r in job["reducers"] for k, v in r["result"]]
    rows.sort(key=(lambda row: (-row[1], row[0])) if job["mode"] == "top10" else (lambda row: row[0]))
    info, counts = flow_info(job) if view == "flow" else ({}, [])
    return render_template("result.html", job=job, rows=rows, view=view,
                           label=LABELS[job["mode"]], info=info, counts=counts)


if __name__ == "__main__":      # required on Windows for the process pool
    app.run(debug=False, port=5000)
