#!/usr/bin/env python3
import json, os, re, time, argparse
from urllib.parse import urlparse
from ddgs import DDGS

AGGREGATOR_BAD = {
    "gograd", "scholarships.com", "scholarshipportal.com", "fastweb.com",
    "prodigyfinance.com", "opportunitiescorners", "bold.org", "ziprecruiter",
    "indeed", "glassdoor", "facebook.com", "twitter.com", "x.com", "instagram.com",
    "youtube.com", "tiktok.com", "reddit.com", "quora.com", "medium.com"
}
TRUST_SUFFIX = (".edu", ".org", ".gov")
ALLOW_BONUS = {
    "aauw.org", "si.edu", "wilsoncenter.org", "lsrf.org", "lucescholars.org",
    "ellisoninstitute.org", "nsf.gov", "neh.gov", "find.uci.edu", "middlebury.edu",
    "ucla.edu", "berkeley.edu", "cornell.edu", "harvard.edu", "whoi.edu", "acls.org",
    "daad.de", "daad.org", "toefl.org", "hertzfoundation.org"
}

def score_url(u: str, title: str, agencies: str) -> float:
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        path = (p.path or "").lower()
    except Exception:
        return -1.0

    score = 0.0
    if p.scheme == "https": score += 2
    if any(b in host for b in AGGREGATOR_BAD): score -= 8
    if host.endswith(TRUST_SUFFIX): score += 5
    for good in ALLOW_BONUS:
        if good in host: score += 5

    title_words = set(w for w in re.split(r"[^a-z0-9]+", title.lower()) if len(w) > 3)
    agency_words = set(w for w in re.split(r"[^a-z0-9]+", (agencies or "").lower()) if len(w) > 3)
    hits = sum(1 for w in title_words if w in u.lower() or w in path)
    score += min(hits, 6) * 1.2
    ahits = sum(1 for w in agency_words if w in u.lower() or w in host or w in path)
    score += min(ahits, 6) * 1.0

    score += max(0, 4 - path.count("/")) * 0.5
    return score

def ddg_search(q: str, max_results: int = 12):
    with DDGS() as ddgs:
        for r in ddgs.text(q, region="wt-wt", safesearch="moderate", max_results=max_results):
            url = r.get("href") or r.get("url")
            if url:
                yield url

def best_url_for(title: str, agencies: str):
    q = f"{title} {agencies}".strip()
    candidates = list(ddg_search(q, max_results=15))
    if not candidates: return None, []

    scored = [(u, score_url(u, title, agencies)) for u in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[0][0], scored[:5]

def enrich(infile: str, outfile: str, sleep: float = 0.5, overwrite: bool = False):
    with open(infile, "r", encoding="utf-8") as f:
        data = json.load(f)
    enriched, report = [], []

    for i, row in enumerate(data, 1):
        title = (row.get("Award Title") or row.get("Title") or "").strip()
        agencies = " ".join([ (row.get("Agency 1") or ""), (row.get("Agency 2") or "") ]).strip()
        cur_url = (row.get("URL") or row.get("Web URL") or row.get("Link") or row.get("Website") or "").strip()

        if cur_url and not overwrite:
            enriched.append(row)
            continue
        if not title:
            enriched.append(row); report.append({"index": i, "reason":"missing-title"}); continue

        url, top = best_url_for(title, agencies)
        if url:
            row = dict(row)
            row["URL"] = url
            enriched.append(row)
            report.append({"index": i, "status":"ok", "picked": url, "candidates": [u for u,_ in top]})
        else:
            enriched.append(row)
            report.append({"index": i, "status":"no-match", "title": title})

        time.sleep(sleep)

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)
    rep = outfile.replace(".json", ".report.json")
    with open(rep, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"✅ Wrote {outfile}\n📝 Wrote {rep}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", default="grapes-data.json")
    ap.add_argument("--out", dest="outfile", default="grapes-data.enriched.json")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.5)
    args = ap.parse_args()
    enrich(args.infile, args.outfile, sleep=args.sleep, overwrite=args.overwrite)

if __name__ == "__main__":
    main()
