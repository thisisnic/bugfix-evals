#!/usr/bin/env python3
"""Append 'base_commit' and 'arrow_version' columns to r_bug_issues.csv.

For each row, finds the latest commit on the main branch of apache/arrow
at or before the issue's creation time, and the latest released Arrow
version at that point. Requires a local clone of apache/arrow with full
history.
"""
from __future__ import annotations

import argparse
import bisect
import re
import subprocess
import sys
from datetime import datetime, timezone

import pandas as pd


def get_commit_at(repo_path: str, before: str) -> str | None:
    """Return the SHA of the latest commit on main at or before `before`."""
    result = subprocess.run(
        ["git", "log", f"--before={before}", "-1", "--format=%H", "main"],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    sha = result.stdout.strip()
    return sha if sha else None


def get_release_tags(repo_path: str) -> list[tuple[datetime, str]]:
    """Return a sorted list of (date, version) for release tags."""
    result = subprocess.run(
        ["git", "tag", "-l", "apache-arrow-[0-9]*", "--sort=creatordate",
         "--format=%(creatordate:iso-strict) %(refname:short)"],
        cwd=repo_path,
        capture_output=True,
        text=True,
    )
    tags = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        date_str, tag = line.split(" ", 1)
        # Skip dev, rc, and js tags
        version = tag.removeprefix("apache-arrow-")
        if re.search(r"(dev|rc|js|-old)", version):
            continue
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        tags.append((dt, version))
    return tags


def get_version_at(tags: list[tuple[datetime, str]], before: str) -> str | None:
    """Return the latest Arrow release version at or before `before`."""
    dt = datetime.fromisoformat(before)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dates = [t[0] for t in tags]
    idx = bisect.bisect_right(dates, dt) - 1
    if idx < 0:
        return None
    return tags[idx][1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        default="r_bug_issues.csv",
        help="Path to r_bug_issues.csv (default: %(default)s)",
    )
    parser.add_argument(
        "--repo",
        default="../arrow",
        help="Path to local apache/arrow clone (default: %(default)s)",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    tags = get_release_tags(args.repo)
    print(f"Found {len(tags)} release tags", file=sys.stderr)

    shas = []
    versions = []
    for _, row in df.iterrows():
        sha = get_commit_at(args.repo, row["issue_created"])
        version = get_version_at(tags, row["issue_created"])
        if sha is None:
            print(
                f"WARNING: no commit found for issue {row['issue_number']} "
                f"(created {row['issue_created']})",
                file=sys.stderr,
            )
        shas.append(sha)
        versions.append(version)
        print(
            f"Issue {row['issue_number']}: {sha[:12] if sha else 'MISSING'}"
            f"  arrow {version or 'MISSING'}",
            file=sys.stderr,
        )

    df["base_commit"] = shas
    df["arrow_version"] = versions
    df.to_csv(args.csv, index=False)
    print(
        f"Wrote {args.csv} with base_commit and arrow_version columns "
        f"({len(df)} rows)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
