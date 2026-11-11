# YDB Tests

Follow the testing style of the module you are modifying.

## C++ unit tests

- Place tests in a `ut/` subdirectory next to the code under test.
- Name test source files `*_ut.cpp`.
- Declare the test module in `ya.make` with `GTEST()`, or
  `UNITTEST()` / `UNITTEST_FOR(...)` when matching existing tests in that `ut/`.
- Register C++ test sources in `SRCS()`.

## Python tests

- Use the `unittest` framework (`unittest.IsolatedAsyncioTestCase` for async code).
- Register Python test files in `TEST_SRCS()` in the module's `ya.make`.
- Third-party packages come from `contrib/python`.
- See [`ydb/tools/mnc/AGENTS.md`](../tools/mnc/AGENTS.md) for MNC-specific
  Python test conventions.

## Functional tests

End-to-end tests that run against a YDB cluster live under `ydb/tests/functional/`.

- Declare targets with `PY3TEST()` in `ya.make`.
- List test files in `TEST_SRCS()` (paths are relative to the repository root).
- Follow existing harness patterns in the same area (`INCLUDE(...harness_dep.inc)`,
  `ENV(...)`, suite `.inc` files).

## Compatibility tests

Upgrade/downgrade tests live under `ydb/tests/compatibility/`.

- Place each test module in the folder matching the feature area.
- Use existing fixtures and follow the layout described in
  [`ydb/tests/compatibility/README.md`](../tests/compatibility/README.md).

## Running tests

From the repository root, build binaries and tests using the
[root build instructions](../../AGENTS.md), then run:

```bash
python3 build/gn/scripts/run_tests.py \
    --build-dir out/gn \
    --script-executable "$(command -v python3)" \
    --test-size small,medium,large \
    --test-threads 8 \
    --strict --filter <tests dir>
```

Replace `<tests dir>` with a repository-relative test directory, such as
`ydb/core/tx/datashard/ut_locks`. The filter matches literal directory substrings,
not test case names or wildcard patterns. Repeat `--filter` to select multiple
directories (a match against any filter is sufficient).
Omit `--filter` to run all selected test sizes, or add `--list` to list tests
without running them.

The GN runner runs C++ tests registered in `//:tests`; recipe-based, manual,
and unbuilt tests are skipped.

Test logs and `summary.json` are in `out/gn/test-results`.

### Selecting cases within a suite

The GN runner has no command-line option for selecting individual cases. After
building, run the test binary directly from the repository root. GN names test
binaries after their directory, replacing `/` with `-`.

For `UNITTEST()` / `UNITTEST_FOR(...)` binaries:

```bash
export ARCADIA_SOURCE_ROOT="$PWD"
export ARCADIA_BUILD_ROOT="$PWD/out/gn"
test_binary=out/gn/ydb-core-tx-datashard-ut_locks

# List available Suite::Test names without running tests.
out/gn/fastld "$test_binary" --list-verbose 2>&1 | tail

# Run one case.
out/gn/fastld "$test_binary" \
    'TDataShardLocksTest::Points_OneTx' 2>&1 | tail

# Run several cases by passing each name separately.
out/gn/fastld "$test_binary" \
    'TDataShardLocksTest::Points_OneTx' \
    'TDataShardLocksTest::Points_ManyTx' 2>&1 | tail

# Run a whole suite except one case.
out/gn/fastld "$test_binary" \
    'TDataShardLocksTest::*' \
    '-TDataShardLocksTest::Points_ManyTx' 2>&1 | tail
```

Use `tail -n 200` (or a larger count) if the case list is truncated. Omit the
`out/gn/fastld` prefix if that launcher has not been built. Direct runs bypass the
GN runner's retries, timeouts, process cleanup, per-run working directories and
`ENV(...)` setup; supply any environment required by the target's `ya.make`.

These unit test filters match exact names. `Suite::*` selects a whole suite;
general wildcard patterns such as `Suite::Points_*` or `Suite::Test?` are not
supported. Quote filters so the shell does not expand `*` or `?`.

For `GTEST()` binaries, list cases with `--gtest_list_tests` and select them with
`--gtest_filter='Suite.Test'`. Google Test supports `*` (any sequence) and `?`
(one character), `:` between alternative patterns, and `-` before exclusions.
For example, `--gtest_filter='Suite.Points_*:Suite.OtherTest-Suite.Points_Slow'`
selects matching cases while excluding `Suite.Points_Slow`.
