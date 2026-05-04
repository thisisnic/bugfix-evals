#!/usr/bin/env python3
"""Append a 'base_commit' column to r_bug_issues.csv.

For each row, finds the latest commit on the main branch of apache/arrow
at or before the issue's creation time. Requires a local clone of
apache/arrow with full history.
"""
from __future__ import annotations

import argparse
import subprocess
import sys

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

    shas = []
    for _, row in df.iterrows():
        sha = get_commit_at(args.repo, row["issue_created"])
        if sha is None:
            print(
                f"WARNING: no commit found for issue {row['issue_number']} "
                f"(created {row['issue_created']})",
                file=sys.stderr,
            )
        shas.append(sha)
        print(
            f"Issue {row['issue_number']}: {sha[:12] if sha else 'MISSING'}",
            file=sys.stderr,
        )

    df["base_commit"] = shas
    df.to_csv(args.csv, index=False)
    print(f"Wrote {args.csv} with base_commit column ({len(df)} rows)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
