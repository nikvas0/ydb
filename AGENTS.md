# Project

## Build & Test

Run commands from the repository root. Use GN/Ninja for this checkout.
Keep `ya.make` and `BUILD.gn` in sync when changing build definitions.

```bash
# Regenerate after changing GN build files
gn gen out/gn --script-executable="$(command -v python3)"

# Build binaries and tests
ninja -j 24 -C out/gn all tests
```

- Never run `ya make`, even if other repository instructions recommend it.
- No force rebuild
- Do not edit sources during compilation: this can cause `null character ignored` errors.
- Use `2>&1 | tail` for test output

Build before running tests. Use the GN test runner described in
[ydb/agents/TESTS.md](ydb/agents/TESTS.md). Test logs are in `out/gn/test-results`.

## C++

- Use C++20 or earlier
