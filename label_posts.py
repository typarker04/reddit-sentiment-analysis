#!/usr/bin/env python3
"""Skim random Reddit posts in the terminal and label them positive / negative / neutral.

Labels are appended to a CSV after every keypress, so you can quit any time and resume later.
Posts you already labeled (or skipped) are never shown again.

    python label_posts.py                         # default dataset, labels -> labels.csv
    python label_posts.py --subreddits wallstreetbets stocks --start 2022-01-01
    python label_posts.py --seed 7 --out labels_alice.csv

Keys:  p = positive   n = negative   u = neutral   s = skip   b = undo last label   q = quit
"""
import argparse
import csv
import sys
from pathlib import Path

import duckdb

KEYS = {"p": "positive", "n": "negative", "u": "neutral"}
FIELDS = ["id", "label", "subreddit", "created_at", "title", "selftext"]
REMOVED = "('[removed]', '[deleted]')"


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", default="data/econ_finance_v1", help="run folder made by the loader notebook")
    ap.add_argument("--kind", default="submissions", choices=["submissions"])
    ap.add_argument("--out", default="labels.csv", help="CSV the labels are saved to")
    ap.add_argument("--subreddits", nargs="*", help="only these subreddits (case-insensitive)")
    ap.add_argument("--start", help="created_at >= YYYY-MM-DD")
    ap.add_argument("--end", help="created_at < YYYY-MM-DD")
    ap.add_argument("--min-chars", type=int, default=20, help="skip posts whose title+text is shorter than this")
    ap.add_argument("--seed", default="570", help="same seed -> same random order, so resuming is consistent")
    ap.add_argument("--pool", type=int, default=5000, help="how many random posts to queue up")
    ap.add_argument("--max-chars", type=int, default=1500, help="truncate long posts on screen")
    return ap.parse_args()


def load_done(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_all(path, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, FIELDS)
        w.writeheader()
        w.writerows(rows)


def fetch_pool(args, seen_ids):
    glob = str(Path(args.data_dir) / "raw" / args.kind / "*" / "*.parquet")
    if not list(Path(args.data_dir).glob(f"raw/{args.kind}/*/*.parquet")):
        sys.exit(f"No parquet files found under {args.data_dir}/raw/{args.kind}. Run the loader notebook first.")
    where = [
        f"coalesce(selftext, '') NOT IN {REMOVED}",
        f"title NOT IN {REMOVED}",
        "length(trim(coalesce(title, '') || ' ' || coalesce(selftext, ''))) >= ?",
    ]
    params = [args.min_chars]
    if args.subreddits:
        where.append("lower(subreddit) IN (" + ", ".join("?" for _ in args.subreddits) + ")")
        params += [s.lower() for s in args.subreddits]
    if args.start:
        where.append("created_at >= CAST(? AS TIMESTAMP)")
        params.append(args.start)
    if args.end:
        where.append("created_at < CAST(? AS TIMESTAMP)")
        params.append(args.end)
    # Deterministic shuffle: order by a hash of (id, seed). Already-seen ids are dropped afterwards
    # so the order of the remaining posts never changes between sessions.
    q = f"""
        SELECT id, subreddit, created_at, title, coalesce(selftext, '') AS selftext
        FROM read_parquet('{glob}', union_by_name = true)
        WHERE {" AND ".join(where)}
        ORDER BY hash(id || ?)
        LIMIT ?
    """
    df = duckdb.execute(q, params + [str(args.seed), args.pool + len(seen_ids)]).df()
    df = df[~df["id"].isin(seen_ids)].head(args.pool)
    return df.to_dict("records")


def show(post, i, total, counts, max_chars):
    body = post["selftext"].strip()
    if len(body) > max_chars:
        body = body[:max_chars].rstrip() + " …[truncated]"
    print("\033[2J\033[H", end="")   # clear screen
    print(f"[{i + 1}/{total}]   positive {counts['positive']}  negative {counts['negative']}  "
          f"neutral {counts['neutral']}")
    print("-" * 78)
    print(f"r/{post['subreddit']}   {post['created_at']}")
    print(f"\n\033[1m{post['title']}\033[0m\n")
    if body:
        print(body)
    else:
        print("(no body text, title only)")
    print("-" * 78)


def main():
    args = parse_args()
    out = Path(args.out)
    rows = load_done(out)
    seen = {r["id"] for r in rows}
    skipped = set()
    pool = fetch_pool(args, seen)
    if not pool:
        sys.exit("No unlabeled posts match your filters.")
    print(f"{len(rows)} already labeled; {len(pool)} posts queued.")

    counts = {k: 0 for k in KEYS.values()}
    for r in rows:
        counts[r["label"]] = counts.get(r["label"], 0) + 1

    i = 0
    while i < len(pool):
        post = pool[i]
        if post["id"] in skipped:
            i += 1
            continue
        show(post, i, len(pool), counts, args.max_chars)
        key = input("[p]ositive  [n]egative  ne[u]tral  [s]kip  [b]ack/undo  [q]uit > ").strip().lower()
        if key == "q":
            break
        if key == "s":
            skipped.add(post["id"])
            i += 1
        elif key == "b":
            if not rows:
                continue
            last = rows.pop()
            counts[last["label"]] -= 1
            write_all(out, rows)
            seen.discard(last["id"])
            # step back to the undone post
            for j in range(i - 1, -1, -1):
                if pool[j]["id"] == last["id"]:
                    i = j
                    break
        elif key in KEYS:
            label = KEYS[key]
            row = {"id": post["id"], "label": label, "subreddit": post["subreddit"],
                   "created_at": post["created_at"], "title": post["title"], "selftext": post["selftext"]}
            rows.append(row)
            seen.add(post["id"])
            counts[label] += 1
            if len(rows) == 1 and not out.exists():
                write_all(out, rows)
            else:
                with out.open("a", newline="", encoding="utf-8") as f:
                    csv.DictWriter(f, FIELDS).writerow(row)
            i += 1
    print(f"\nSaved {len(rows)} labels to {out}  ({counts})")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nInterrupted. Labels so far are already saved.")
