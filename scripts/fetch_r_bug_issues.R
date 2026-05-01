#!/usr/bin/env Rscript

# Fetch the most recent 30 closed issues from apache/arrow that:
#   * are labelled "Component: R"
#   * are labelled "Type: bug"
#   * have a merged PR associated with them (via Apache Arrow's
#     "GH-<issue>" referencing convention used in PR titles/bodies)
#   * were NOT opened by a regular project committer
#
# Committer status is derived from the cache published by
# thisisnic/arrow-gh-cache: a user is treated as a regular committer if any
# of their merged PRs in the cache carry an author_association of MEMBER,
# OWNER, or COLLABORATOR, or if they have authored at least
# COMMITTER_MIN_MERGED_PRS merged PRs.

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(purrr)
  library(tibble)
})

CACHE_BASE <- "https://github.com/thisisnic/arrow-gh-cache/releases/download/cache-latest"
COMPONENT_LABEL <- "Component: R"
BUG_LABEL <- "Type: bug"
N_ISSUES <- 30
COMMITTER_ASSOCIATIONS <- c("MEMBER", "OWNER", "COLLABORATOR")
COMMITTER_MIN_MERGED_PRS <- 5

download_cache <- function(name) {
  dest <- file.path(tempdir(), paste0(name, ".parquet"))
  if (!file.exists(dest)) {
    url <- paste0(CACHE_BASE, "/", name, ".parquet")
    message("Downloading ", url)
    utils::download.file(url, dest, mode = "wb", quiet = TRUE)
  }
  read_parquet(dest)
}

has_label <- function(labels, target) {
  vapply(labels, function(lbls) target %in% lbls, logical(1))
}

# --- Load cached data ---------------------------------------------------------
closed_issues <- download_cache("closed_issues")
closed_prs    <- download_cache("closed_prs")

# --- Identify regular committers ---------------------------------------------
# Anyone who has authored a merged PR while flagged MEMBER/OWNER/COLLABORATOR,
# plus anyone with a high count of merged PRs.
merged_prs <- closed_prs |>
  filter(!is.na(merge_commit_sha), !is.na(merged_at))

committers_by_assoc <- merged_prs |>
  filter(author_association %in% COMMITTER_ASSOCIATIONS) |>
  pull(user_login) |>
  unique()

committers_by_volume <- merged_prs |>
  count(user_login, name = "n_merged") |>
  filter(n_merged >= COMMITTER_MIN_MERGED_PRS) |>
  pull(user_login)

committers <- unique(c(committers_by_assoc, committers_by_volume))
committers <- committers[!is.na(committers)]
message("Identified ", length(committers), " committer logins from cache")

# --- Filter R bug issues ------------------------------------------------------
r_bug_issues <- closed_issues |>
  filter(
    has_label(labels, COMPONENT_LABEL),
    has_label(labels, BUG_LABEL)
  )
message("Found ", nrow(r_bug_issues), " closed R bug issues in cache")

# --- Match issues to merged PRs via "GH-<num>" references --------------------
# Apache Arrow PRs reference the issue they close in the title (and/or body)
# using the form "GH-<issue-number>". We also match GitHub's standard
# "Closes/Fixes/Resolves #<num>" syntax in PR bodies.
extract_gh_refs <- function(text) {
  if (is.na(text)) return(integer(0))
  m <- c(
    str_match_all(text, "(?i)GH[-#](\\d+)")[[1]][, 2],
    str_match_all(text, "(?i)(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\\s*[:\\-]?\\s*#(\\d+)")[[1]][, 2]
  )
  m <- m[!is.na(m)]
  if (length(m) == 0) return(integer(0))
  unique(as.integer(m))
}

pr_refs <- merged_prs |>
  mutate(refs = map2(title, body, ~ unique(c(extract_gh_refs(.x), extract_gh_refs(.y))))) |>
  select(pr_number = number, pr_user = user_login, pr_html_url = html_url,
         pr_title = title, merged_at, refs) |>
  unnest_longer(refs) |>
  filter(!is.na(refs)) |>
  rename(issue_number = refs)

# Keep only PRs that reference one of our R-bug issues; for issues with
# multiple matching merged PRs, keep the earliest merged one.
issue_pr_match <- pr_refs |>
  semi_join(r_bug_issues, by = c("issue_number" = "number")) |>
  group_by(issue_number) |>
  arrange(merged_at, .by_group = TRUE) |>
  slice(1) |>
  ungroup()

# --- Compose final output -----------------------------------------------------
result <- r_bug_issues |>
  inner_join(issue_pr_match, by = c("number" = "issue_number")) |>
  filter(!user_login %in% committers) |>
  arrange(desc(created_at)) |>
  slice_head(n = N_ISSUES) |>
  transmute(
    issue_number   = number,
    issue_title    = title,
    issue_user     = user_login,
    issue_created  = created_at,
    issue_closed   = closed_at,
    issue_url      = html_url,
    pr_number,
    pr_user,
    pr_merged_at   = merged_at,
    pr_url         = pr_html_url,
    pr_title
  )

message("Returning ", nrow(result), " issues")

out_path <- "r_bug_issues.csv"
args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 1) out_path <- args[[1]]
write.csv(result, out_path, row.names = FALSE)
message("Wrote ", out_path)

print(result)
