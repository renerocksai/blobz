//! First attempt
//!
//! Uses a global mutex for the hash map (zig's hashmaps aren't threadsafe)
//! Retrieved values always hold a lock: either for .reading or for .writing.
//!

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Opts = struct {
    save_interval_seconds: usize,
    prefix: []const u8,
    workdir: []const u8,

    pub const default: Opts = .{
        .save_interval_seconds = 10,
        .prefix = "objectstore",
        .workdir = ".",
    };
};

/// Create the Blobz object store.
///
/// You own the keys. They are not copied or duped.
/// Unmanaged: you provide an allocator when it's needed.
///
/// Values that you read
pub fn Store(K: type, V: type) type {
    return struct {
        _kv_store: std.AutoArrayHashMapUnmanaged(K, Wrap(V)) = .empty,
        // dang. wish zig's hashmaps were threadsafe
        _insert_mutex: std.Thread.Mutex = .{},

        pub const Self = @This();
        pub const RetrieveMode = enum { reading, writing };

        /// Return value of the Blobz store
        /// It contains a pointer to the retrieved value associated with the key.
        /// It is already read-locked when it is returned to you.
        /// You must call ReadValue.unlock() when finished:
        ///
        /// ```zig
        /// const alloc = std.testing.allocator;
        ///
        /// var store : Blobz.Store([]const u8, usize) = .{};
        /// try store.put(alloc, 0, 1000);
        ///
        /// const v = store.getValuePtrLocked(0) orelse unreachable;
        /// defer v.unlock();
        ///
        /// std.debug.print("Value is: {d}\n", .{ v.value_ptr.* });
        /// ```
        pub const RetrievedValue = struct {
            _rw_mode: RetrieveMode,
            _lock: *std.Thread.RwLock,
            value_ptr: *V,

            pub fn unlock(self: *RetrievedValue) void {
                if (self._rw_mode == .reading) {
                    self._lock.unlockShared();
                } else {
                    self._lock.unlock();
                }
            }
        };

        pub fn ensureCapacity(self: *Self, alloc: Allocator, capacity: usize) !void {
            self._kv_store.ensureTotalCapacity(alloc, capacity);
        }

        pub fn deinit(self: *Self, alloc: Allocator) void {
            self._kv_store.deinit(alloc);
        }

        /// call .unlock() on the returned RetrievedValue wrapper
        /// use rw_mode .writing if you plan to modify any internals of the
        /// returned value
        pub fn getValueFor(self: *Self, k: K, rw_mode: RetrieveMode) ?RetrievedValue {
            self._insert_mutex.lock();
            defer self._insert_mutex.unlock();
            if (self._kv_store.getPtr(k)) |wrapped_ptr| {
                if (rw_mode == .reading) {
                    wrapped_ptr._rw_lock.lockShared();
                } else {
                    wrapped_ptr._rw_lock.lock();
                }
                return .{
                    ._lock = &wrapped_ptr._rw_lock,
                    .value_ptr = &wrapped_ptr.value,
                    ._rw_mode = rw_mode,
                };
            } else {
                return null;
            }
        }

        pub fn exists(self: *Self, key: K) bool {
            return self._kv_store.contains(key);
        }

        /// For now, this is used to put in or replace values.
        /// For the time of replacing an element with a new value, the
        /// value will be write-locked
        ///
        /// if you modified a value returned by getValueFor(..., .writing),
        /// then make sure you call value.unlock() before calling upsert
        pub fn upsert(self: *Self, alloc: Allocator, key: K, value: V) !void {

            // TODO: check if we can get away without
            self._insert_mutex.lock();
            defer self._insert_mutex.unlock();

            const gopResult = try self._kv_store.getOrPut(alloc, key);
            if (gopResult.found_existing) {
                // we need to replace
                // prevent reading and writing from/to this value while we replace
                gopResult.value_ptr.*._rw_lock.lock();
                defer gopResult.value_ptr.*._rw_lock.unlock();
                gopResult.value_ptr.*._dirty_time = std.time.nanoTimestamp();
                // we overwrite the value
                gopResult.value_ptr.*.value = value;
            } else {
                // we need to insert:
                // this is safe since lookups will not happen while we
                // modify the value, because of _insert_mutex
                gopResult.value_ptr.* = .{
                    ._dirty_time = std.time.nanoTimestamp(),
                    ._collection_time = std.time.nanoTimestamp(), // never
                    .value = value,
                };
            }
        }

        /// For now, this is used to put in or replace values.
        /// For the time of replacing an element with a new value, the
        /// value will be write-locked
        ///
        /// if you modified a value returned by getValueFor(..., .writing),
        /// then make sure you call value.unlock() before calling upsert
        pub fn upsertAssumeCapacity(self: *Self, key: K, value: V) !void {
            var wrapped: Wrap(V) = .{
                ._dirty_time = std.time.nanoTimestamp(),
                ._collection_time = std.time.nanoTimestamp(), // never
                .value = value,
            };

            // if we insert, we insert locked
            wrapped._rw_lock.lock();

            // TODO: check if we can get away without
            self._insert_mutex.lock();
            defer self._insert_mutex.unlock();

            const gopResult = self._kv_store.getOrPutAssumeCapacity(wrapped, key);
            if (gopResult.found_existing) {
                // we need to replace
                // prevent reading and writing from/to this value while we replace
                gopResult.value_ptr.*._rw_lock.lock();
                defer gopResult.value_ptr.*._rw_lock.unlock();
                gopResult.value_ptr.*._dirty_time = std.time.nanoTimestamp();
                // we overwrite the value
                gopResult.value_ptr.*.value = value;
            } else {
                // we inserted
                gopResult.value_ptr.*._rw_lock.unlock();
            }
        }
    };
}

pub fn Wrap(V: type) type {
    return struct {
        _rw_lock: std.Thread.RwLock = .{},
        /// nanotimestamp when item was modified
        _dirty_time: i128,
        /// nanotimestamp when item was last collected
        _collection_time: i128,

        /// the wrapped value
        value: V,
    };
}

test "1" {
    const o: Opts = .default;
    std.debug.print("Opts: {}\n", .{o});
}

test "2" {
    const alloc = std.testing.allocator;
    var store: Store(usize, usize) = .{};
    defer store.deinit(alloc);
    const key: usize = 1;
    try store.upsert(alloc, key, 1000);

    var v = store.getValueFor(key, .reading) orelse unreachable;
    defer v.unlock();
    std.debug.print("Value v is: {d}\n", .{v.value_ptr.*});

    var vv = store.getValueFor(key, .reading) orelse unreachable;
    std.debug.print("NO DEADLOCK!!!", .{});
    defer vv.unlock();
    std.debug.print("Value vv is: {d}\n", .{vv.value_ptr.*});
}
