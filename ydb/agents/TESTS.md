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
    --test-threads 4 \
    --strict --filter <tests dir>
```

Replace `<tests dir>` with a repository-relative test directory, such as
`ydb/core/tx/datashard/ut_locks`. The filter matches directory substrings.
Omit `--filter` to run all selected test sizes, or add `--list` to list tests
without running them.

The GN runner runs C++ tests registered in `//:tests`; recipe-based, manual,
and unbuilt tests are skipped.

Test logs and `summary.json` are in `out/gn/test-results`.
