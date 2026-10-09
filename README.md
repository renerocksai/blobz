# Blobz — Your wobbly Garbage Persistor

An experimental in-memory key-value store with periodic JSON persistence in a
background thread. Build with exact **Zig 0.17.0**, recorded in `.zig-version`.
Read the original introduction [here](https://renerocks.ai/blog/blobz).

```sh
zig build verify -Doptimize=debug
zig build verify -Doptimize=safe
```

Exported module: `blobz`. `Store(K, V)` supports integer and byte-slice keys.
This release integrates the compatibility and ownership work previously vendored
in [Furhat/Vershofen](https://github.com/technologylab-ai/furhat-vershofen).
Its JSON layout, xxHash64 seed, shard paths and package fingerprint are preserved;
existing store files require no conversion. Furhat keeps its vendored path
package. [MIGRATION.md](MIGRATION.md) records provenance and changed signatures.

## Lifetime and shutdown

Create a store with `Store(K, V).init(allocator, io, opts)`, passing the caller's
`std.Io`. The I/O implementation, init allocator, option strings, keys and
nested value storage must outlive the store, background saver and every retrieved value. The store
owns its map and stable value wrappers. It does not copy keys or own allocations
inside values. The allocator from `init` is used for map/wrapper storage;
allocator arguments on the existing mutation/deinit methods remain for source
compatibility. `upsertAssumeCapacity` can still allocate a value wrapper.

`getValueFor(key, .reading)` and `.writing` return a locked borrowed value;
call `unlock()` before replacing it or releasing the store. A write borrow does
not automatically mark a value dirty: after unlocking, use `upsert` to record the
update. Stable wrappers keep retrieved pointers valid when the map grows.
Direct map access must hold `_insert_mutex` and respect value locks.

Stop application work and return its borrows, call `stopPersistorThread()`, then
`flush()` before `deinit(allocator)`. The flush persists the last update without
waiting for the configured interval and returns failures. The saver wakes and
joins on stop; repeating stop is harmless. Failed writes remain dirty for retry.
Writes replace JSON files directly; this is not a transactional or crash-durable
store. Thread creation and I/O must stay outside an application's request I/O
loop.

The tests cover numeric and hashed persistence, original JSON fixtures, stable
borrows across growth, failed-write retry, background updates and saver
stop/restart. Native platform qualification is recorded in MIGRATION.md.
