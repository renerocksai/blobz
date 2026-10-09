# Zig 0.17 migration of blobz 0.0.5

Vendored from the web application's original dependency:

- Upstream: https://github.com/renerocksai/blobz
- Commit: `cf2bd4974a12c5f40bfdd9438dfc46dc98cdc7eb` (`v0.0.5`)
- Zig package hash: `blobz-0.0.5-3vwmOKrFAABgRlTOAoTBSc5E3YqLsZk8mJn3WY2O8ten`
- The original MIT license is preserved in `LICENSE`.

The original Zig 0.14 upstream was ported locally to Zig 0.16 in Furhat/Vershofen.
That vendored implementation is now reconciled into the real Blobz repository
and both copies target exact Zig 0.17.0. The upstream version, fingerprint and
MIT license remain unchanged. Furhat still uses its local path dependency, so a
clean application checkout contains the exact patched sources.

`Store.init(gpa, io, opts)` receives and retains the caller's `std.Io` and owning
allocator. `Persistor.init(io, config)` and `NumericKeyPersistor.init(io, config)`
also receive explicit I/O. Existing `upsert(gpa, ...)`, `ensureCapacity(gpa, ...)`
and `deinit(gpa)` signatures remain, but map and wrapper allocations always use
the allocator from `init`; keys and nested values are still caller-owned.
`RetrievedValue.unlock()` retains its signature and uses the captured I/O.
`Store.flush()` synchronously attempts every dirty entry and returns the first
failure after the loop; successful entries are collected and failures stay dirty
for retry. Call it after stopping request workers and joining the saver for a
final shutdown flush. Loading preserves the inclusive `persist.max_file_size`
bound, including a non-overflowing translation of `maxInt(usize)`.
Custom formatting uses the `{f}` formatter.

Filesystem operations, locks, timestamps, and saver wakeups use `std.Io`.
`_kv_store` holds `*Wrap(V)` so growth cannot invalidate a retrieved value or its
lock. Code accessing map internals must hold `_insert_mutex` and iterate values
with `|v|`, not `|*v|`. The saver copies keys before releasing the map mutex,
reads timestamps under the value lock, releases locks on allocation failure,
and marks a value collected only after a successful write. Shutdown wakes and
joins the saver before storage is freed, and repeated stops are harmless.

Persistence remains compatible: unchanged xxHash64 seed, sharding, hexadecimal
filenames, integer-key value JSON, and string-key `{ "key", "value" }` JSON.
No conversion of existing object-store files is required.

Validation: run `zig build verify -Doptimize=debug` and
`zig build verify -Doptimize=safe` with exact Zig 0.17.0. Tests cover
existing numeric/hashed persistence and background saving, legacy JSON fixtures,
map growth while a retrieved value holds a lock, saver shutdown/restart, and
flushing the last mutation without waiting for a long save interval, progress
past a failed first entry, and actual filesystem loads at and above the limit.


## Zig 0.17 review

`Config.shard_levels_default` preserves slice descriptors' legacy physical
memory width with `@sizeOf(K) * 8`, because slices have no logical `@bitSizeOf`
in Zig 0.17. This keeps the existing four hashed-key shard levels on 64-bit
hosts; the numeric key widths remain logical integer widths. Existing fixed
path and legacy-file fixtures supply independent layout oracles.

The reflection helpers use unchanged pointer size/child fields; no qualifier or
field-index association is discarded. There are no `@bitCast` or `@hasDecl`
consumers in these sources. All six source modules participate in the test
root. `verify` checks formatting, builds the exported static library and runs
all tests. Native path fixture expectations follow the target separator.

## Native verification — 2026-10-09

At code revision `2abb0294584af4d7a0a8f718ba650e3c820cbc47`,
[publication CI](https://github.com/renerocksai/blobz/actions/runs/37941260055)
passed exact Zig 0.17.0 `zig build verify` in Debug and Safe on every host below.
Each mode passed all 6 build steps and all 11 tests, including legacy hashed
paths/JSON, borrowed values across growth, saver stop/restart and failed-flush
retry. These are native execution results, with separate platform packets.

| Native host | Environment recorded in the packet |
| --- | --- |
| macOS ARM64 | macOS 26.6.2; runner image `20260907.0351.1` |
| Linux x86-64 | kernel `6.17.0-1022-azure`, glibc 2.39; image `20261004.327.1` |
| Windows x86-64 | Windows Server 2025, build `10.0.26100`; image `20260925.250.1` |

The first compile identified the removed logical slice bit size; the initial
repair then needed an explicit compile-time type branch. Both failed attempts
remain attached to their original CI revisions. The final branch preserves the
legacy layout rather than substituting the 64-bit hash width for slice-key
configuration. Additional registry/kernel metadata is captured by subsequent
workflow runs. Earlier Zig 0.16 application results remain historical evidence.
No performance, crash-durability, or arbitrary application-isolation claim is
made by these correctness gates.
