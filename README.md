# bugfix-evals

A small dataset of recent Apache Arrow R-package bug issues, intended as
seed material for bugfix evaluation work.

## What's here

- `scripts/fetch_r_bug_issues.py` — pulls the cached `apache/arrow`
  issues and PRs from
  [`thisisnic/arrow-gh-cache`](https://github.com/thisisnic/arrow-gh-cache)
  and produces a CSV of recent R-package bug reports that were closed
  by a merged PR and were *not* opened by a regular project committer.
- `r_bug_issues.csv` — the output, with `root_cause` and `solution`
  columns added by hand per row (see below).

## How `r_bug_issues.csv` was built

1. **Fetch the cache.** `fetch_r_bug_issues.py` downloads
   `closed_issues.parquet` and `closed_prs.parquet` from the
   `cache-latest` release of `thisisnic/arrow-gh-cache`.

2. **Filter to R bugs.** Keep closed issues whose `labels` list contains
   both `Component: R` and `Type: bug`.

3. **Identify regular committers** from the cache so we can exclude
   them as issue authors:
   - any `user_login` whose merged PR carries `author_association` in
     `{MEMBER, OWNER, COLLABORATOR}`, OR
   - any `user_login` with at least `COMMITTER_MIN_MERGED_PRS` (= 5)
     merged PRs in the cache.

4. **Match issues to merged PRs.** For each merged PR
   (`merge_commit_sha` not null), extract issue references from the
   title and body using the Apache Arrow `GH-<num>` convention plus
   GitHub's `Closes/Fixes/Resolves #<num>` syntax. Keep the
   earliest-merged PR per issue.

5. **Compose the output.** Inner-join issues to their matched PR,
   exclude issues opened by committers, sort by `created_at`
   descending, and take the top 30.

   To reproduce just this part:

   ```bash
   pip install pandas pyarrow
   python3 scripts/fetch_r_bug_issues.py r_bug_issues.csv
   ```

6. **Add `root_cause` and `solution`.** For each of the 30 PRs the
   description and diff were read manually (via PR URLs of the form
   `https://github.com/apache/arrow/pull/<num>/files`) and a 2–3
   sentence root-cause + solution summary was written into the CSV.
   This step is not automated — the columns are stored in the CSV as
   data, not regenerated.

## Caveats

- "Regular committer" is defined heuristically from the cache; the
  threshold is tunable at the top of the script.
