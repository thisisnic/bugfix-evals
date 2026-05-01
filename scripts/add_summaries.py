"""Add root_cause and solution columns to r_bug_issues.csv.

Summaries were obtained by reading each PR description and diff (via the
PR template's "Rationale for this change" / "What changes are included
in this PR?" sections, augmented with diff inspection).
"""
from __future__ import annotations

import pandas as pd

SUMMARIES: dict[int, dict[str, str]] = {
    49619: {
        "root_cause": "`GetVectorType()` in `r/src/r_to_arrow.cpp` only checked for `POSIXct` in the `REALSXP` branch. In R 4.5.2+, zero-length `POSIXct` is now stored as `INTSXP` (integer), so it was misclassified as `INT32` instead of timestamp.",
        "solution": "Added `else if (Rf_inherits(x, \"POSIXct\")) { return POSIXCT; }` to the `INTSXP` branch of `GetVectorType()` so any `POSIXct` is mapped to the timestamp type regardless of underlying storage. Tests added in `test-Array.R` and `test-parquet.R`.",
    },
    49608: {
        "root_cause": "`as.data.frame.Schema()` in `r/R/schema.R` called `as.data.frame(Table__from_schema(x))`, which propagated Arrow table metadata (e.g. named-vector column attrs) onto the intermediate data frame and triggered an \"Invalid metadata$r\" warning on later operations like `rename_with()`.",
        "solution": "Replaced the call with `Table__from_schema(x)$to_data_frame()` (line 470) to use Arrow's native conversion and avoid carrying the metadata across. Regression test added to `test-dplyr-select.R`.",
    },
    48104: {
        "root_cause": "`apply_arrow_r_metadata()` in `r/R/metadata.R` entered its per-column loop whenever `columns_metadata` was non-NULL, even if every entry inside was `NULL`, causing an O(ncol) walk over plain data frames with no per-column metadata.",
        "solution": "Tightened the guard at line 178 to `if (length(names(x)) && !is.null(columns_metadata) && !all(map_lgl(columns_metadata, is.null)))`, skipping the loop entirely when there is no per-column metadata to apply.",
    },
    47278: {
        "root_cause": "In `r/R/dplyr-funcs-datetime.R`, `register_bindings_hms()`'s `numeric_to_time32()` cast to `time32(unit = \"s\")`, dropping subsecond precision. Numeric values weren't scaled, and string parsing used `\"%Y-%m-%d-%H-%M-%S\"`.",
        "solution": "Switched to `time32(unit = \"ms\")`, multiplied numeric inputs by 1000 via `Expression$create(\"multiply_checked\", x, 1000)`, and corrected the strptime format to `\"%Y-%m-%d %H:%M:%S\"`. Added tests asserting nanosecond/subsecond character inputs error explicitly.",
    },
    47217: {
        "root_cause": "The R package required CMake >= 3.25 (`r/DESCRIPTION` SystemRequirements and `ensure_cmake()` default in `r/tools/nixlibs.R`), but bundled Apache Thrift now needs CMake >= 3.26, so install failed on Debian 12 where 3.25 was being picked up.",
        "solution": "Bumped the minimum to 3.26 in both `r/DESCRIPTION` (`cmake >= 3.26`) and the `cmake_minimum_required` default in `r/tools/nixlibs.R::ensure_cmake()`.",
    },
    47885: {
        "root_cause": "`concat_tables()` in `r/R/table.R` did not handle `RecordBatch` inputs; passing one bypassed the Table-conversion path and caused a segfault during concatenation.",
        "solution": "Added an `lapply()` over inputs that wraps any `RecordBatch` in `arrow_table()` before concatenation (lines 192-198 of `table.R`), leaving Tables unchanged. Documentation and tests for the new RecordBatch input were added.",
    },
    46834: {
        "root_cause": "Documentation and build files still required CMake 3.16, even though the project's actual minimum had moved on. `r/DESCRIPTION` declared `cmake >= 3.16`, `docs/source/developers/cpp/building.rst` said \"CMake 3.16 or higher\", and `cpp/cmake_modules/ThirdpartyToolchain.cmake` carried an obsolete `CMAKE_VERSION VERSION_LESS 3.22` workaround block.",
        "solution": "Bumped all CMake-version references to 3.25: `r/DESCRIPTION` -> `cmake >= 3.25`, `building.rst` -> \"CMake 3.25\", `cpp/examples/minimal_build/CMakeLists.txt` `cmake_minimum_required(VERSION 3.25)`, and removed the now-redundant CMake 3.22 fallback block from `ThirdpartyToolchain.cmake`.",
    },
    46667: {
        "root_cause": "`arrow_eval()` in `r/R/dplyr-eval.R` invoked `eval_tidy(expr, mask)` without passing `env`, so when `case_when()` (or similar) referenced an object defined outside the global env it could not be looked up via the data mask.",
        "solution": "Changed line 33 to `eval_tidy(expr, mask, env = mask)`, so the data mask is also used as the lookup environment. This makes externally-defined symbols resolve correctly inside dplyr expressions.",
    },
    46346: {
        "root_cause": "`ReadRangeCache::Impl::Cache` in `cpp/src/arrow/io/caching.cc` propagated every error from `file->WillNeed(ranges)` as fatal. On sshfs 3.7.3, `F_RDADVISE` returns an I/O error, so opening a dataset failed even though prefetching is only an optimisation.",
        "solution": "Captured the `WillNeed()` status and downgraded I/O errors to success: if `st.IsIOError()` the function now returns `Status::OK()` instead of propagating, allowing dataset opening to continue when prefetch isn't supported.",
    },
    44675: {
        "root_cause": "Three R tests used `expect_true(all(x == y))` patterns which broke under dev `testthat`, where `expect_true()` no longer tolerated the resulting comparison semantics; one parquet test additionally hit a timestamp-unit mismatch that `==` quietly accepted.",
        "solution": "Replaced `expect_true(all(x == y))` with `expect_equal(x, y)` in `test-Array.R` (lines 1265, 1286), `test-extension.R` (line 38) and `test-parquet.R` (line 458). The parquet test also gained an explicit `.cast()` call to align timestamp units before comparison.",
    },
    44094: {
        "root_cause": "`to_arrow()` (DuckDB -> Arrow handoff) in `r/R/duckdb.R` returned a `RecordBatchReader` that can only be consumed once, but the documentation (`r/man/to_arrow.Rd`) didn't say so, leading users to call `collect()`/`compute()` twice and hit confusing failures.",
        "solution": "This was a docs-only fix: added a note to `to_arrow()`'s Roxygen docs (and the regenerated `.Rd`) explaining that `collect()`/`compute()` may only be called once on the result, and recommending either making `collect()` the final step or materialising via `as_arrow_table()` first.",
    },
    44141: {
        "root_cause": "The `str_sub` binding in `r/R/dplyr-funcs-string.R` only guarded against `end < start`, ignoring the indexing-semantics mismatch between R's `stringr::str_sub` (1-based, inclusive end) and Arrow's `utf8_slice_codeunits` (0-based, exclusive end). Negative `end` values therefore silently produced a substring one character short of the user's intent.",
        "solution": "Replaced the simple `end < start` check with logic that adds 1 to negative `end` values when `end < -1` to convert R's inclusive negative indices into Arrow's exclusive end positions, restoring parity with `stringr::str_sub`.",
    },
    43895: {
        "root_cause": "`check_r_metadata_types_recursive()` in `r/R/metadata.R` recursed via `[[` on subclassed lists. Subclasses with custom `[[` methods (e.g. `packageVersion`, which is a list with class `\"package_version\"`) caused the recursion to never terminate when an attribute carried such an object.",
        "solution": "Wrapped the recursion target in `unclass(x)` so subclass-specific `[[` methods don't intercept the traversal, then reapplied the original attributes after validation. Added a regression test using `packageVersion()` in metadata.",
    },
    43889: {
        "root_cause": "Several docs (`developers/r/resources.rst`, `developers/r/r_tutorial.rst`, `developers/r/arrow_codebase.rst`, `r/vignettes/developing.Rmd`) linked to a \"Writing Bindings\" vignette and a related walkthrough section that no longer exists, producing 404s.",
        "solution": "Docs-only cleanup that deletes ~35 lines of stale references: removed link/seealso/note blocks pointing at the bindings vignette, dropped the entire \"Writing Bindings Walkthrough\" section in `r_tutorial.rst`, and removed the bullet from `developing.Rmd`.",
    },
    43446: {
        "root_cause": "In `r/R/dplyr-funcs-conditional.R`, `register_bindings_conditional()` cast the `%in%` value set to the column's full `DictionaryType`. Arrow's `is_in` kernel expects the value-type, not the dictionary type itself, so filtering a factor (dictionary-encoded) column with `%in%` errored out.",
        "solution": "Detect `DictionaryType` and unwrap it before casting: when `inherits(x_type, \"DictionaryType\")`, set `x_type <- x_type$value_type` and pass that to `cast_or_parse()`, so the value set is cast to the dictionary's underlying value-type.",
    },
    43338: {
        "root_cause": "`r/` had a backlog of lintr violations across many files: `any(is.na(x))` instead of `anyNA(x)`, decimals without a leading zero (e.g. `.05`), `expect_identical(names(x), ...)` instead of `expect_named()`, and an unnecessary `ifelse(..., TRUE, FALSE)` wrapper in `r/R/dplyr-funcs-datetime.R`.",
        "solution": "Pure-style cleanup: swapped `any(is.na(x))` -> `anyNA(x)` in `arrow-tabular.R`, `dplyr-datetime-helpers.R` and tests; rewrote `.05/.1/.5/.99` literals with the leading zero; replaced `expect_identical(names(...), ...)` with `expect_named(...)` across many test files; and simplified `ifelse(call_binding(\"is.Date\", x), TRUE, FALSE)` to a direct assignment in `dplyr-funcs-datetime.R:817`.",
    },
    43162: {
        "root_cause": "`Math.ArrowDatum` in `r/R/arrow-datum.R` lumped several Math-group generics into a fall-through that just called `.Generic`, so `log2`, `log1p`, `cumprod`, `cummin`, and `cummax` had no real Arrow binding and either errored or gave wrong results on Array/ChunkedArray.",
        "solution": "Wrote explicit per-function bindings: `log2` -> `\"log2_checked\"`, `log1p` -> `\"log1p_checked\"`, and the cumulative generics to their respective Arrow compute functions. Added new test cases in `test-compute-arith.R` covering Array and ChunkedArray inputs.",
    },
    47599: {
        "root_cause": "R's `write_dataset()` had no way to skip directory creation; the C++ `FileSystemDatasetWriteOptions::create_dir` flag wasn't exposed, so partitioned writes always tried to create the partition directories. On S3 buckets where the user lacked `ListBucket`, this failed even though the write itself was permitted.",
        "solution": "Plumbed a `create_directory = TRUE` argument through the R API (`r/R/dataset-write.R`), regenerated bindings (`r/src/arrowExports.cpp`), and forwarded it in `r/src/compute-exec.cpp` via `opts.create_dir = create_directory`, so users can opt out of directory creation when permissions are restricted.",
    },
    42188: {
        "root_cause": "`compare_internal_avx2.cc` used the AVX2 gather intrinsics `_mm256_i32gather_epi32`/`_epi64`, which interpret their offset operand as *signed* int32. Once a row table exceeded 2 GB, offsets above 0x80000000 were interpreted as negative, so `CompareColumnsToRows` read from the wrong addresses and segfaulted.",
        "solution": "Introduced `UnsignedOffsetSafeGather32`/`Gather64` helpers that shift the base pointer +2 GB and the offset -2 GB, keeping the offset in `[-2 GB, 2 GB)` while pointing at the right address. All direct gather calls in `compare_internal_avx2.cc` were routed through these wrappers; a >2 GB regression test was added.",
    },
    41019: {
        "root_cause": "On macOS, the CRAN binary of `arrow` ships with several features disabled, so `.onAttach()` in `r/R/arrow-package.R` printed a confusing \"some features are off\" warning even when only jemalloc was off, and gave no actionable next step. There was also no guidance pointing users to the fully-built r-universe binaries.",
        "solution": "Restructured `.onAttach()` to detect macOS and recommend reinstalling from r-universe via `install.packages('arrow', repos = 'https://apache.r-universe.dev')`, and excluded `\"jemalloc\"` from the `some_features_are_off()` blocklist in `r/R/arrow-info.R` to suppress the spurious warning.",
    },
    49714: {
        "root_cause": "`InferArrowTypeFromVector` in `r/src/type_infer.cpp` read a `POSIXct`'s `tzone` attribute without checking its type. If `tzone` was something other than a character vector (e.g. produced by `as.POSIXct(1, NA)`), the code proceeded into Arrow internals and surfaced a cryptic `STRING_ELT() can only be applied to a 'character vector'` error far from the real cause.",
        "solution": "Added `else if (TYPEOF(tzone_sexp) != STRSXP) cpp11::stop(\"`tzone` attribute of a `POSIXct` vector must be a character vector\")` to both the `INTSXP` and `REALSXP` specialisations of `InferArrowTypeFromVector`, so invalid `tzone` values fail fast with a clear message.",
    },
    49713: {
        "root_cause": "`to_arrow()` in `r/R/duckdb.R` round-tripped data through DuckDB but threw away `dplyr` grouping; the returned object had no groups, breaking subsequent `summarise()`/`mutate()` calls that relied on `group_by()` set before the call.",
        "solution": "Captured groups with `groups <- dplyr::groups(.data)` before executing the DuckDB query, and reapplied them on the result via `if (length(groups)) out <- dplyr::group_by(out, !!!groups)`. Added a roundtrip test confirming groups survive `to_arrow()`.",
    },
    40610: {
        "root_cause": "The pkgdown site's dark-theme navbar inherited insufficient text contrast: the navbar links, version badge, and search-input placeholder rendered nearly invisible against the dark background.",
        "solution": "Added a 14-line `r/pkgdown/extra.css` overriding `.navbar-dark`/`.navbar[data-bs-theme=\"dark\"]`, `.nav-text`/`.text-muted`, and `#search-input::placeholder` to use `#d9d9d9`, restoring legibility in the navbar.",
    },
    40232: {
        "root_cause": "`create_package_with_all_dependencies()` in `r/R/install-arrow.R` repacked vendored deps with `utils::tar(..., compression = \"gz\")`. With no `extra_flags` specified, R's default flags strip executable bits, so configure scripts inside the resulting tarball lost their `+x` permission.",
        "solution": "Passed `extra_flags = NULL` to the `utils::tar()` call (line 254), preventing R's default flag set from being applied and thereby preserving executable permissions on the bundled files.",
    },
    39892: {
        "root_cause": "When the CSV reader hit cell values containing newlines and the user hadn't enabled `newlines_in_values`, the chunker and parser silently desynced and `BlockParsingOperator::operator()` / `ReaderMixin::Parse()` in `cpp/src/arrow/csv/reader.cc` returned only a generic \"got out of sync\" error, with no actionable hint.",
        "solution": "Added an explicit check for `parsed_size < bytes_before_buffer` in both call sites and surfaced a targeted error: \"contains cell values spanning multiple lines; please consider enabling the option 'newlines_in_values'.\" Added regression tests in `parser_test.cc` and `test_csv.py` for truncated-view handling and the new error message.",
    },
    45719: {
        "root_cause": "`readr_to_csv_convert_options()` in `r/R/csv.R` had inline logic for parsing readr-style compact column-type specs (e.g. `\"idlcfT\"`), which was hard to reuse and untested. `open_delim_dataset()`'s `col_types` documentation also didn't show the compact-string form.",
        "solution": "Extracted the parsing into reusable helpers `parse_compact_col_spec()` and `col_type_from_compact()` in `r/R/util.R`, replaced the inline implementation in `csv.R` with calls into them, expanded `open_delim_dataset` docs to demonstrate both schema and compact-string forms, and added unit tests in `test-util.R`.",
    },
    39219: {
        "root_cause": "`arrow_r_string_replace_function` in `r/R/dplyr-funcs-string.R` did not validate `pattern`/`replacement` lengths, so passing length>1 vectors to `stringr::str_replace_all()` bindings was silently accepted and produced incorrect output instead of an error like in stringr.",
        "solution": "Added `length(pattern) != 1` / `length(replacement) != 1` guards at the top of the binding that throw an error, and added matching tests in `test-dplyr-funcs-string.R`.",
    },
    38897: {
        "root_cause": "`RConnectionFileInterface::Tell()` in `r/src/io.cpp` always called R's `seek()` to report position. Non-seekable connections (e.g. socket connections) error on `seek()`, so `RecordBatchStreamWriter` couldn't be constructed over a socket connection.",
        "solution": "Added `seekable_`, `bytes_written_`, and `bytes_read_` state to `RConnectionFileInterface` plus a `check_seekable()` helper that probes via R's `isSeekable()`. `Tell()` now returns the running byte counter for non-seekable connections instead of calling `seek()`, and `WriteBase()`/`ReadBase()` keep the counter up to date.",
    },
    38495: {
        "root_cause": "`arrow_duck_connection()` in `r/R/duckdb.R` opened a DuckDB connection but never registered cleanup; relying on `.onUnload()`/`.onDetach()` was unreliable, so users saw \"Connection is garbage-collected, use dbDisconnect() to avoid this\" warnings after `to_duckdb()`.",
        "solution": "Created an environment object `arrow_duck_finalizer` and attached `reg.finalizer()` to it, calling `DBI::dbDisconnect(con, shutdown = TRUE)` on namespace teardown. This guarantees the DuckDB connection is closed cleanly without depending on package-detach hooks.",
    },
    37961: {
        "root_cause": "`on_rosetta()` was defined in `r/R/arrow-package.R` but called from `r/R/install-arrow.R`. R's collation order meant the function wasn't yet visible at install time on macOS Rosetta, so the install path errored before it could detect a Rosetta environment.",
        "solution": "Moved `on_rosetta()` from `arrow-package.R` to `install-arrow.R` so it's defined alongside its only caller, refactored its single result into a variable to avoid recomputation, and added a test in `test-install-arrow.R` confirming it runs without warnings.",
    },
}


def main() -> None:
    df = pd.read_csv("r_bug_issues.csv")
    df["root_cause"] = df["pr_number"].map(lambda n: SUMMARIES[int(n)]["root_cause"])
    df["solution"] = df["pr_number"].map(lambda n: SUMMARIES[int(n)]["solution"])
    df.to_csv("r_bug_issues.csv", index=False)
    print(f"Updated {len(df)} rows; columns now: {list(df.columns)}")


if __name__ == "__main__":
    main()
