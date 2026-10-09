const std = @import("std");
const blobz = @import("blobz.zig");
const persist = @import("persist.zig");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;

pub const Opts = struct {
    sleep_time_ms: usize = 250,
    locking_spin_time_ms: usize = 2,
    locking_spin_max_count: usize = 5,

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
    log_alive_message_interval_ms: i64 = 0,
};

const log = std.log.scoped(.save_thread);

pub fn SaveThread(K: type, V: type) type {
    return struct {
        blobz_store: *blobz.Store(K, V),
        opts: Opts,
        thread: ?std.Thread = null,
        exit_signal: std.Io.Event = .unset,
        arena_state: ArenaAllocator,

        const Self = @This();
        const WrappedValue = blobz.Store(K, V).WrappedValue_Type;
        // Values have stable allocations; copy the key because map storage can move.
        const DirtyItem = struct { key: K, wrapped_ptr: *WrappedValue };

        pub fn init(allocator: Allocator, blobz_store: *blobz.Store(K, V), opts: Opts) Self {
            return .{
                .arena_state = ArenaAllocator.init(allocator),
                .blobz_store = blobz_store,
                .opts = opts,
            };
        }

        pub fn start(self: *Self) !void {
            if (self.thread != null) return error.AlreadyStarted;
            self.exit_signal = .unset;
            self.thread = try std.Thread.spawn(.{}, Self.thread_main, .{self});
        }

        pub fn stop(self: *Self) void {
            self.exit_signal.set(self.blobz_store.io);
        }

        pub fn stopAndWait(self: *Self) void {
            self.stop();
            if (self.thread) |thread| {
                thread.join();
                self.thread = null;
                const allocator = self.arena_state.child_allocator;
                self.arena_state.deinit();
                self.arena_state = ArenaAllocator.init(allocator);
            }
        }

        fn thread_main(self: *Self) void {
            const io = self.blobz_store.io;
            const arena = self.arena_state.allocator();
            var last_alive_log_time: i64 = 0;
            var last_collection_time: i96 = 0;
            while (!self.exit_signal.isSet()) {
                self.exit_signal.waitTimeout(io, .{ .duration = .{
                    .raw = .fromMilliseconds(@intCast(self.opts.sleep_time_ms)),
                    .clock = .awake,
                } }) catch |err| switch (err) {
                    error.Timeout => {},
                    error.Canceled => return,
                };
                if (self.exit_signal.isSet()) break;
                _ = self.arena_state.reset(.retain_capacity);
                const collection_time = std.Io.Timestamp.now(io, .real).nanoseconds;
                const now_ms: i64 = @intCast(@divTrunc(collection_time, std.time.ns_per_ms));
                if (self.opts.log_alive_message_interval_ms > 0 and
                    now_ms >= last_alive_log_time + self.opts.log_alive_message_interval_ms)
                {
                    log.info("alive.", .{});
                    last_alive_log_time = now_ms;
                }
                if (collection_time < last_collection_time + self.blobz_store.opts.save_interval_seconds * std.time.ns_per_s) continue;
                last_collection_time = collection_time;
                var dirty_values: std.ArrayList(DirtyItem) = .empty;
                {
                    self.blobz_store._insert_mutex.lockUncancelable(io);
                    defer self.blobz_store._insert_mutex.unlock(io);
                    var it = self.blobz_store._kv_store.iterator();
                    while (it.next()) |entry| {
                        const wrapped = entry.value_ptr.*;
                        // Do not inspect timestamps until the value lock is held.
                        var attempts: usize = 0;
                        while (!wrapped._rw_lock.tryLock(io)) {
                            attempts += 1;
                            if (attempts >= self.opts.locking_spin_max_count) break;
                            io.sleep(.fromMilliseconds(@intCast(self.opts.locking_spin_time_ms)), .awake) catch break;
                        } else {
                            if (wrapped._dirty_time >= wrapped._collection_time) {
                                dirty_values.append(arena, .{ .key = entry.key_ptr.*, .wrapped_ptr = wrapped }) catch {
                                    wrapped._rw_lock.unlock(io);
                                    break;
                                };
                                continue;
                            }
                            wrapped._rw_lock.unlock(io);
                        }
                    }
                }
                const config = persist.Config.initDefault(K, self.blobz_store.dest_path);
                var persistor = persist.Persistor(K, V).init(io, config);
                for (dirty_values.items) |item| {
                    defer item.wrapped_ptr._rw_lock.unlock(io);
                    persistor.persist(arena, item.key, item.wrapped_ptr.value) catch |err| {
                        log.err("Unable to persist key {any}: {}", .{ item.key, err });
                        continue; // Keep it dirty so the next collection retries.
                    };
                    item.wrapped_ptr._collection_time = collection_time;
                }
            }
        }
    };
}

// let's test this
test SaveThread {
    const fsutils = @import("fsutils.zig");

    const alloc = std.testing.allocator;
    const io = std.testing.io;

    // What goes into the store
    const KEY_TYPE = u16;
    const BASE_PATH = ",,test_save_thread";
    const PREFIX = "u16store";

    const Value = struct {
        first_name: []const u8,
        last_name: []const u8,

        pub fn deinit(self: *const @This(), allocator: Allocator) void {
            allocator.free(self.first_name);
            allocator.free(self.last_name);
        }
    };

    // empty the directory just in case
    try std.Io.Dir.cwd().deleteTree(io, BASE_PATH);

    // the store
    var store = try blobz.Store(KEY_TYPE, Value).init(alloc, io, .{
        .prefix = PREFIX,
        .workdir = BASE_PATH,
        .initial_capacity = 1000,
        .save_interval_seconds = 1,
        .log_alive_message_interval_ms = 1000,
    });
    defer store.deinit(alloc);
    defer std.Io.Dir.cwd().deleteTree(io, BASE_PATH) catch unreachable;

    // some values
    const value_1: Value = .{ .first_name = "rene", .last_name = "rocksai" };
    var value_2: Value = .{ .first_name = "your", .last_name = "mom" };

    // start a save thread
    var t = SaveThread(KEY_TYPE, Value).init(alloc, &store, .{
        .log_alive_message_interval_ms = 1000,
    });
    try t.start();
    defer t.stopAndWait();

    // let's test
    //
    // time step 1: dir exists, but no files
    try io.sleep(.fromMilliseconds(@intCast((store.opts.save_interval_seconds + 1) * 1000)), .awake);
    try std.testing.expectEqual(true, fsutils.isDirPresent(io, BASE_PATH ++ "/" ++ PREFIX));
    try std.testing.expectEqual(false, fsutils.fileExists(io, BASE_PATH ++ "/" ++ PREFIX ++ "/00/01/0001.json"));
    try std.testing.expectEqual(false, fsutils.fileExists(io, BASE_PATH ++ "/" ++ PREFIX ++ "/00/02/0002.json"));

    // time step 2: dir exists AND first file exists
    try store.upsert(alloc, 1, value_1);

    try io.sleep(.fromMilliseconds(@intCast((store.opts.save_interval_seconds + 1) * 1000)), .awake);
    try std.testing.expectEqual(true, fsutils.isDirPresent(io, BASE_PATH ++ "/" ++ PREFIX));
    try std.testing.expectEqual(true, fsutils.fileExists(io, BASE_PATH ++ "/" ++ PREFIX ++ "/00/01/0001.json"));
    try std.testing.expectEqual(false, fsutils.fileExists(io, BASE_PATH ++ "/" ++ PREFIX ++ "/00/02/0002.json"));

    // time step 3: dir exists AND both files exist
    try store.upsert(alloc, 2, value_2);
    try io.sleep(.fromMilliseconds(@intCast((store.opts.save_interval_seconds + 1) * 1000)), .awake);
    try std.testing.expectEqual(true, fsutils.isDirPresent(io, BASE_PATH ++ "/" ++ PREFIX));
    try std.testing.expectEqual(true, fsutils.fileExists(io, BASE_PATH ++ "/" ++ PREFIX ++ "/00/01/0001.json"));
    try std.testing.expectEqual(true, fsutils.fileExists(io, BASE_PATH ++ "/" ++ PREFIX ++ "/00/02/0002.json"));

    // time step 4: update a value, and check if its file contents reflect the change
    value_2.first_name = "my";
    try store.upsert(alloc, 2, value_2);
    try io.sleep(.fromMilliseconds(@intCast((store.opts.save_interval_seconds + 1) * 1000)), .awake);
    try std.testing.expectEqual(true, fsutils.isDirPresent(io, BASE_PATH ++ "/" ++ PREFIX));
    try std.testing.expectEqual(true, fsutils.fileExists(io, BASE_PATH ++ "/" ++ PREFIX ++ "/00/01/0001.json"));
    try std.testing.expectEqual(true, fsutils.fileExists(io, BASE_PATH ++ "/" ++ PREFIX ++ "/00/02/0002.json"));
    const content = try std.Io.Dir.cwd().readFileAlloc(io, BASE_PATH ++ "/" ++ PREFIX ++ "/00/02/0002.json", alloc, .limited(1024));
    defer alloc.free(content);
    var parsed = try std.json.parseFromSlice(Value, alloc, content, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("my", parsed.value.first_name);
    try std.testing.expectEqualStrings("mom", parsed.value.last_name);
}
