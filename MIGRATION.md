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
`Store.flush()` synchronously writes dirty entries and returns failures; call it
after stopping request workers and joining the saver for a final shutdown flush.
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
flushing the last mutation without waiting for a long save interval.


## Zig 0.17 review

The reflection helpers use unchanged pointer size/child fields; no qualifier or
field-index association is discarded. There are no `@bitCast` or `@hasDecl`
consumers in these sources. All six source modules participate in the test
root. `verify` checks formatting, builds the exported static library and runs
all tests. Native path fixture expectations follow the target separator.

The current port's platform results will be recorded after the named gates run.
Earlier Zig 0.16 application results do not qualify this compiler upgrade.
