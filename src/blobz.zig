//! First attempt
//!
//! Uses a global mutex for the hash map (zig's hashmaps aren't threadsafe)
//! Retrieved values always hold a lock: either for .reading or for .writing.
//!

pub const version = @import("version.zig").version() orelse "(unknown version)";
pub const SaveThread = @import("savethread.zig").SaveThread;

const std = @import("std");
const Allocator = std.mem.Allocator;

const log = std.log.scoped(.blobz);

pub const Opts = struct {
    initial_capacity: usize,
    save_interval_seconds: usize,
    /// The "prefix" or name of this very object store inside the working
    /// directory.
    prefix: []const u8,
    /// The working directory. Subdirs will be created from here on, starting
    /// with the `prefix` subdir.
    workdir: []const u8,

    pub const default: Opts = .{
        .initial_capacity = 1000,
        .save_interval_seconds = 10,
        .prefix = "default",
        .workdir = "object_store",
    };

    pub fn format(
        self: Opts,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try writer.print(
            ".{{.initial_capacity = {d}, .save_interval_seconds = {d}, .prefix = \"{s}\", .workdir = \"{s}\" }}",
            .{ self.initial_capacity, self.save_interval_seconds, self.prefix, self.workdir },
        );
    }
};

/// Create the Blobz object store.
///
/// You own the keys. They are not copied or duped.
/// Unmanaged: you provide an allocator when it's needed.
///
/// Values that you read
pub fn Store(K: type, V: type) type {
    return struct {
        opts: Opts = .default,
        dest_path: []const u8,

        _kv_store: std.AutoArrayHashMapUnmanaged(K, Wrap(V)),

        // dang. wish zig's hashmaps were threadsafe
        _insert_mutex: std.Thread.Mutex = .{},

        pub const Key_Type: type = K;
        pub const Value_Type: type = V;
        pub const WrappedValue_Type: type = Wrap(V);

        const Self = @This();

        pub fn format(
            self: *const Self,
            comptime fmt: []const u8,
            options: std.fmt.FormatOptions,
            writer: anytype,
        ) !void {
            _ = fmt;
            _ = options;
            try writer.print(
                ".{{ .opts = {}, .dest_path = \"{s}\", ._kv_store={{.count = {d}, .capacity = {d}}}, }}",
                .{ self.opts, self.dest_path, self._kv_store.count(), self._kv_store.capacity() },
            );
        }

        /// Return value of the Blobz store
        /// It contains a pointer to the retrieved value associated with the key.
        /// It is already read-locked when it is returned to you.
        /// You must call ReadValue.unlock() when finished.
        pub const RetrievedValue = struct {
            _rw_mode: RetrieveMode,
            _lock: *std.Thread.RwLock,
            value_ptr: *V,

            pub fn unlock(self: *RetrievedValue) void {
                switch (self._rw_mode) {
                    .reading => self._lock.unlockShared(),
                    .writing => self._lock.unlock(),
                }
            }
        };
        pub const RetrieveMode = enum { reading, writing };

        pub fn init(gpa: Allocator, opts: Opts) !Self {
            // create the directory for the store
            const dest_path = try std.fs.path.join(gpa, &.{ opts.workdir, opts.prefix });
            std.fs.cwd().makePath(dest_path) catch |err| {
                log.err("Unable to create destination path `{s}`: {}", .{ dest_path, err });
                return err;
            };

            var ret: Self = .{
                .opts = opts,
                .dest_path = dest_path,
                ._kv_store = .empty,
            };
            try ret.ensureCapacity(gpa, opts.initial_capacity);
            return ret;
        }

        pub fn deinit(self: *Self, gpa: Allocator) void {
            gpa.free(self.dest_path);
            self._kv_store.deinit(gpa);
        }

        pub fn ensureCapacity(self: *Self, gpa: Allocator, capacity: usize) !void {
            try self._kv_store.ensureTotalCapacity(gpa, capacity);
        }

        /// call .unlock() on the returned RetrievedValue wrapper
        /// use rw_mode .writing if you plan to modify any internals of the
        /// returned value
        pub fn getValueFor(self: *Self, k: K, rw_mode: RetrieveMode) ?RetrievedValue {
            self._insert_mutex.lock();
            defer self._insert_mutex.unlock();
            if (self._kv_store.getPtr(k)) |wrapped_ptr| {
                switch (rw_mode) {
                    .reading => wrapped_ptr._rw_lock.lockShared(),
                    .writing => wrapped_ptr._rw_lock.lock(),
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
        pub fn upsert(self: *Self, gpa: Allocator, key: K, value: V) !void {

            // TODO: check if we can get away without
            self._insert_mutex.lock();
            defer self._insert_mutex.unlock();

            const gopResult = try self._kv_store.getOrPut(gpa, key);
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

// TODO: re-think this. Does it make values unnecessary large?
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

test "2" {
    const alloc = std.testing.allocator;

    var store = try Store(usize, usize).init(alloc, .default);
    defer store.deinit(alloc);

    std.debug.print("The store is: {} (v{s})\n", .{ store, version });

    const key: usize = 1;
    try store.upsert(alloc, key, 1000);

    var v = store.getValueFor(key, .reading) orelse unreachable;
    defer v.unlock();
    std.debug.print("Value v is: {d}\n", .{v.value_ptr.*});

    var vv = store.getValueFor(key, .reading) orelse unreachable;
    std.debug.print("NO DEADLOCK!!!\n", .{});
    defer vv.unlock();
    std.debug.print("Value vv is: {d}\n", .{vv.value_ptr.*});
}
