//! First attempt
//!
//! Uses a global mutex for the hash map (zig's hashmaps aren't threadsafe)
//! Retrieved values always hold a lock: either for .reading or for .writing.
//!

pub const version_str = @import("version.zig").version() orelse "(unknown version)";
pub const SaveThread = @import("savethread.zig").SaveThread;
pub const persist = @import("persist.zig");

const meta = @import("meta.zig");

// for including tests in other modules:
comptime {
    _ = @import("savethread.zig");
    _ = @import("persist.zig");
}

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
    /// From SaveThread's Opts:
    /// if > 0, logs that the thread is alive every n milliseconds.
    /// Note: the accuracy is influenced by:
    ///       - sleep_time_ms
    ///       - locking_spin_time_ms (and locking_spin_max_count)
    ///       - the current persistor load
    ///
    ///       While the thread is sleeping or busy persisting, it won't be able
    ///       to check whether it should log an alive message.
    ///       For simplicity and to avoid unnecessary wakeups from sleep,
    ///       logging is done in the thread's main loop.
    log_alive_message_interval_ms: i64,

    pub const default: Opts = .{
        .initial_capacity = 1000,
        .save_interval_seconds = 10,
        .prefix = "default",
        .workdir = "object_store",
        .log_alive_message_interval_ms = 0,
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

        _kv_store: KV_Store_Type,
        // dang. wish zig's hashmaps were threadsafe
        _insert_mutex: std.Thread.Mutex = .{},

        persistor_thread: ?SaveThread(K, V),

        pub const Key_Type: type = K;
        pub const Value_Type: type = V;
        pub const WrappedValue_Type: type = Wrap(V);
        pub const KV_Store_Type: type = if (meta.isSliceOf(K, u8)) std.StringArrayHashMapUnmanaged(Wrap(V)) else std.AutoArrayHashMapUnmanaged(K, Wrap(V));

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
                .persistor_thread = null,
            };
            try ret.ensureCapacity(gpa, opts.initial_capacity);
            return ret;
        }

        pub fn deinit(self: *Self, gpa: Allocator) void {
            if (self.persistor_thread) |*t| {
                t.stopAndWait();
            }
            gpa.free(self.dest_path);
            self._kv_store.deinit(gpa);
        }

        pub fn ensureCapacity(self: *Self, gpa: Allocator, capacity: usize) !void {
            try self._kv_store.ensureTotalCapacity(gpa, capacity);
        }

        pub fn startPersistorThread(self: *Self, gpa: Allocator) !void {
            self.persistor_thread = SaveThread(K, V).init(gpa, self, .{
                .log_alive_message_interval_ms = self.opts.log_alive_message_interval_ms,
            });
            try self.persistor_thread.?.start();
        }

        pub fn stopPersistorThread(self: *Self) void {
            if (self.persistor_thread) |*t| {
                t.stopAndWait();
                self.persistor_thread = null;
            }
        }

        const ErrorNotFound = error{NotFound};

        /// call .unlock() on the returned RetrievedValue wrapper
        /// use rw_mode .writing if you plan to modify any internals of the
        /// returned value
        pub fn getValueFor(self: *Self, k: K, rw_mode: RetrieveMode) ErrorNotFound!RetrievedValue {
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
                return error.NotFound;
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
                    ._collection_time = 0, // never
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
                ._collection_time = 0, // never
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

        /// Try to load all files into the store, return an owned list of failed files
        pub fn loadFromDisk(self: *Self, trash_arena: Allocator, value_arena: Allocator) !?[][]const u8 {
            const config = persist.Config.initDefault(K, self.dest_path);
            const Persistor = persist.Persistor(K, V);
            var persistor = Persistor.init(config);

            // now iterate over all .json files in all subdirs
            var base_dir = try std.fs.cwd().openDir(
                self.dest_path,
                .{ .iterate = true },
            );
            defer base_dir.close();
            var walker = try base_dir.walk(trash_arena);
            defer walker.deinit(); // it's trashed but still

            var error_files = std.ArrayListUnmanaged([]const u8).empty;
            defer error_files.deinit(trash_arena);

            // TODO: can we react to iter errors in a meaningful way?
            //       can, in our usecase where we don't use symlinks or other
            //       stuff, even assume that there may be non-fatal errors?
            var path_buf: [std.fs.max_path_bytes]u8 = undefined;
            while (try walker.next()) |entry| {
                // check if the path ends in `.json`
                if (std.mem.endsWith(u8, entry.path, ".json")) {
                    const full_path = try std.fmt.bufPrint(
                        &path_buf,
                        "{s}/{s}",
                        .{ self.dest_path, entry.path },
                    );
                    const kv: Persistor.KV = persistor.loadFromPath(value_arena, full_path) catch |err| {
                        log.err("Unable to load {s} : {}", .{ entry.path, err });
                        try error_files.append(trash_arena, entry.path);
                        continue;
                    };

                    try self.upsert(value_arena, kv.key, kv.value);
                }
            }

            return if (error_files.items.len == 0) null else try error_files.toOwnedSlice(trash_arena);
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

test "rw_lock: reading" {
    const alloc = std.testing.allocator;

    var store = try Store(usize, usize).init(alloc, .default);
    defer store.deinit(alloc);

    const key: usize = 1;
    try store.upsert(alloc, key, 1000);

    var v = try store.getValueFor(key, .reading);
    defer v.unlock();
    try std.testing.expectEqual(1000, v.value_ptr.*);

    var vv = try store.getValueFor(key, .reading);
    // std.debug.print("NO DEADLOCK!!!\n", .{});
    defer vv.unlock();
    try std.testing.expectEqual(1000, vv.value_ptr.*);
}

test "string ids" {
    const alloc = std.testing.allocator;

    var store = try Store([]const u8, usize).init(alloc, .default);
    defer store.deinit(alloc);

    const key = "hello";
    try store.upsert(alloc, key, 1000);

    var v = try store.getValueFor(key, .reading);
    defer v.unlock();
    try std.testing.expectEqual(1000, v.value_ptr.*);

    var vv = try store.getValueFor(key, .reading);
    // std.debug.print("NO DEADLOCK!!!\n", .{});
    defer vv.unlock();
    try std.testing.expectEqual(1000, vv.value_ptr.*);
}

test "Load From Hashing Persistor" {

    // first, we just construct a store's file "system", then try to load it

    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const KEY_TYPE = []const u8;
    const BASE_PATH = ",,test_loading_from_hashing_persistor";
    const PREFIX = "hashing_persistor";

    const Value = struct {
        my_key_field: []const u8,
        first_name: []const u8,
        last_name: []const u8,

        pub fn deinit(self: *const @This(), allocator: Allocator) void {
            allocator.free(self.my_key_field);
            allocator.free(self.first_name);
            allocator.free(self.last_name);
        }
    };

    // now, create the store and load it
    var store = try Store(KEY_TYPE, Value).init(gpa, .{
        .prefix = PREFIX,
        .workdir = BASE_PATH,
        .initial_capacity = 1000,
        .save_interval_seconds = 1,
        .log_alive_message_interval_ms = 1000,
    });
    defer store.deinit(gpa);
    defer std.fs.cwd().deleteTree(BASE_PATH) catch unreachable;

    // just for fun!
    try store.startPersistorThread(gpa);
    defer store.stopPersistorThread();

    const config = persist.Config.initDefault(KEY_TYPE, store.dest_path);

    // persist some values
    var persistor = persist.Persistor(KEY_TYPE, Value).init(config);

    const value_1: Value = .{ .my_key_field = "user 1", .first_name = "rene", .last_name = "rocksai" };
    const value_2: Value = .{ .my_key_field = "user 2", .first_name = "your", .last_name = "mom" };
    try persistor.persist(gpa, value_1.my_key_field, value_1);
    try persistor.persist(gpa, value_2.my_key_field, value_2);

    if (try store.loadFromDisk(arena, arena)) |error_files| {
        for (error_files) |error_file| {
            std.debug.print("\nError loading file {s}\n", .{error_file});
        }

        // re-raise, so test fails after printing
        try std.testing.expectEqual(null, error_files);
    }

    var read_value_1 = try store.getValueFor("user 1", .reading);
    defer read_value_1.unlock();
    try std.testing.expectEqualStrings(value_1.first_name, read_value_1.value_ptr.first_name);
    try std.testing.expectEqualStrings(value_1.last_name, read_value_1.value_ptr.last_name);

    var read_value_2 = try store.getValueFor("user 2", .reading);
    defer read_value_2.unlock();
    try std.testing.expectEqualStrings(value_2.first_name, read_value_2.value_ptr.first_name);
    try std.testing.expectEqualStrings(value_2.last_name, read_value_2.value_ptr.last_name);
}
