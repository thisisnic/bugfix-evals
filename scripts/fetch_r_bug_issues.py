#!/usr/bin/env python3
"""Fetch the most recent 30 closed issues from apache/arrow that:

  * are labelled "Component: R"
  * are labelled "Type: bug"
  * have a merged PR associated with them (via Apache Arrow's "GH-<num>"
    referencing convention used in PR titles/bodies, plus GitHub's
    standard "Closes/Fixes/Resolves #<num>" syntax)
  * were NOT opened by a regular project committer

Committer status is derived from the cache published by
thisisnic/arrow-gh-cache: a user is treated as a regular committer if any
of their merged PRs in the cache carry an author_association of MEMBER,
OWNER, or COLLABORATOR, or if they have authored at least
COMMITTER_MIN_MERGED_PRS merged PRs.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
import urllib.request
from collections import Counter

import pandas as pd

CACHE_BASE = "https://github.com/thisisnic/arrow-gh-cache/releases/download/cache-latest"
COMPONENT_LABEL = "Component: R"
BUG_LABEL = "Type: bug"
N_ISSUES = 30
COMMITTER_ASSOCIATIONS = {"MEMBER", "OWNER", "COLLABORATOR"}
COMMITTER_MIN_MERGED_PRS = 5

GH_REF_RE = re.compile(r"GH[-#](\d+)", re.IGNORECASE)
CLOSES_RE = re.compile(
    r"(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s*[:\-]?\s*#(\d+)",
    re.IGNORECASE,
)


def download_cache(name: str) -> pd.DataFrame:
    dest = os.path.join(tempfile.gettempdir(), f"{name}.parquet")
    if not os.path.exists(dest):
        url = f"{CACHE_BASE}/{name}.parquet"
        print(f"Downloading {url}", file=sys.stderr)
        urllib.request.urlretrieve(url, dest)
    return pd.read_parquet(dest)


def has_label(labels, target: str) -> bool:
    if labels is None:
        return False
    try:
        return target in list(labels)
    except TypeError:
        return False


def extract_refs(text) -> set[int]:
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return set()
    refs: set[int] = set()
    for m in GH_REF_RE.findall(text):
        refs.add(int(m))
    for m in CLOSES_RE.findall(text):
        refs.add(int(m))
    return refs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", nargs="?", default="r_bug_issues.csv")
    args = parser.parse_args()

    closed_issues = download_cache("closed_issues")
    closed_prs = download_cache("closed_prs")

    # --- Identify regular committers -----------------------------------
    merged_prs = closed_prs[
        closed_prs["merge_commit_sha"].notna() & closed_prs["merged_at"].notna()
    ].copy()

    committers_by_assoc = set(
        merged_prs.loc[
            merged_prs["author_association"].isin(COMMITTER_ASSOCIATIONS),
            "user_login",
        ].dropna().unique()
    )

    pr_counts = Counter(merged_prs["user_login"].dropna())
    committers_by_volume = {
        u for u, n in pr_counts.items() if n >= COMMITTER_MIN_MERGED_PRS
    }

    committers = committers_by_assoc | committers_by_volume
    print(
        f"Identified {len(committers)} committer logins from cache "
        f"({len(committers_by_assoc)} by association, "
        f"{len(committers_by_volume)} by volume)",
        file=sys.stderr,
    )

    # --- Filter R bug issues -------------------------------------------
    is_r = closed_issues["labels"].apply(lambda x: has_label(x, COMPONENT_LABEL))
    is_bug = closed_issues["labels"].apply(lambda x: has_label(x, BUG_LABEL))
    r_bug = closed_issues[is_r & is_bug].copy()
    print(f"Found {len(r_bug)} closed R bug issues in cache", file=sys.stderr)

    # --- Match issues to merged PRs via references --------------------
    # Build a long-form mapping of merged PR -> referenced issue numbers,
    # then keep only refs that match an R-bug issue, then pick the
    # earliest-merged PR per issue.
    valid_issue_numbers = set(r_bug["number"].astype(int).tolist())

    rows = []
    for _, pr in merged_prs.iterrows():
        refs = extract_refs(pr.get("title")) | extract_refs(pr.get("body"))
        refs &= valid_issue_numbers
        for issue_number in refs:
            rows.append(
                {
                    "issue_number": issue_number,
                    "pr_number": pr["number"],
                    "pr_user": pr["user_login"],
                    "pr_url": pr["html_url"],
                    "pr_title": pr["title"],
                    "pr_merged_at": pr["merged_at"],
                }
            )

    if not rows:
        print("No issue/PR matches found", file=sys.stderr)
        return 1

    issue_pr = pd.DataFrame(rows)
    issue_pr = (
        issue_pr.sort_values("pr_merged_at")
        .drop_duplicates("issue_number", keep="first")
    )

    # --- Compose final output ------------------------------------------
    merged = r_bug.merge(
        issue_pr, left_on="number", right_on="issue_number", how="inner"
    )
    merged = merged[~merged["user_login"].isin(committers)]
    merged = merged.sort_values("created_at", ascending=False).head(N_ISSUES)

    result = merged[
        [
            "number",
            "title",
            "user_login",
            "created_at",
            "closed_at",
            "html_url",
            "pr_number",
            "pr_user",
            "pr_merged_at",
            "pr_url",
            "pr_title",
        ]
    ].rename(
        columns={
            "number": "issue_number",
            "title": "issue_title",
            "user_login": "issue_user",
            "created_at": "issue_created",
            "closed_at": "issue_closed",
            "html_url": "issue_url",
        }
    )

    print(f"Returning {len(result)} issues", file=sys.stderr)
    result.to_csv(args.output, index=False)
    print(f"Wrote {args.output}", file=sys.stderr)

    pd.set_option("display.max_colwidth", 80)
    pd.set_option("display.width", 200)
    print(
        result[
            ["issue_number", "issue_user", "issue_title", "pr_number", "pr_user"]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
