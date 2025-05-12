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
        save_interval_seconds: usize,
        locking_spin_time_ms: usize,
        locking_spin_max_count: usize,
        log_alive_message_interval_ms: i64,
        sleep_time_ms: usize,
        blobz_store: *blobz.Store(K, V),

        thread: std.Thread = undefined,
        exit_signal: std.Thread.ResetEvent = .{},

        last_save_time: i128 = 0,

        arena_state: ArenaAllocator,

        const Self = @This();
        const WrappedValue = blobz.Store(K, V).WrappedValue_Type;
        const DirtyItem = struct { key_ptr: *const K, wrapped_ptr: *WrappedValue };

        pub fn init(allocator: Allocator, blobz_store: *blobz.Store(K, V), opts: Opts) Self {
            return .{
                .arena_state = ArenaAllocator.init(allocator),
                .blobz_store = blobz_store,
                .save_interval_seconds = blobz_store.opts.save_interval_seconds,
                .sleep_time_ms = opts.sleep_time_ms,
                .locking_spin_time_ms = opts.locking_spin_time_ms,
                .locking_spin_max_count = opts.locking_spin_max_count,
                .log_alive_message_interval_ms = opts.log_alive_message_interval_ms,
            };
        }

        pub fn start(self: *Self) !void {
            self.thread = try std.Thread.spawn(.{}, Self.thread_main, .{self});
        }

        pub fn stop(self: *Self) void {
            self.exit_signal.set();
        }

        pub fn stopAndWait(self: *Self) void {
            self.exit_signal.set();
            self.thread.join();
        }

        fn thread_main(self: *Self) !void {
            const arena = self.arena_state.allocator();
            defer self.arena_state.deinit();

            var last_alive_log_time: i64 = 0;
            var last_collection_time: i128 = 0;

            while (!self.exit_signal.isSet()) {
                _ = self.arena_state.reset(.retain_capacity); // we don't care if it went OK

                if (self.log_alive_message_interval_ms > 0) {
                    if (std.time.milliTimestamp() + self.log_alive_message_interval_ms > last_alive_log_time) {
                        log.info("alive.", .{});
                        last_alive_log_time = std.time.milliTimestamp();
                    }
                }

                // delay so we don't hog the CPU
                std.time.sleep(self.sleep_time_ms * std.time.ns_per_ms);

                const collection_time = std.time.nanoTimestamp();

                // let's honor the save_interval_seconds
                if (collection_time < last_collection_time + self.save_interval_seconds * std.time.ns_per_s) {
                    continue;
                } else {
                    last_collection_time = collection_time;
                }

                var dirty_values = std.ArrayListUnmanaged(DirtyItem).empty;

                // iterate over all blobz objects and
                {
                    // stop the world - before doing anything else
                    // FIXME: don't stop the world
                    self.blobz_store._insert_mutex.lock();
                    defer self.blobz_store._insert_mutex.unlock();

                    // find the dirty ones and:
                    //      - record, in a list, their addresses which is safe
                    //        because they can't be deleted from the blobz
                    //        store while the _insert_mutex is held.
                    //
                    //        Note that we currently don't even support
                    //        deleting values in the blobz store anyway!
                    //
                    //      - acquire their write locks so they are protected
                    //        from modification and deletion (which we don't
                    //        support anyway).
                    //
                    //        This assumes that a future delete operation would
                    //        wait on the value's rw lock before deletion and
                    //        subsequent potential destruction to ensure no
                    //        write / modify-transaction is currently underway.
                    //
                    //        Note: some expensive "upsert" or rather
                    //        getValueFor(.writing) transaction even of a
                    //        single value could delay this entire
                    //        stop-the-world operation.
                    //
                    //      - speaking of deletion: we should probably just
                    //        append to a `free`-list and do the deletion
                    //        *here*? Which would upgrade this thread from a
                    //        mere saving thread to a maintenance thread.
                    //
                    //      - By using a free-list, we don't need as much
                    //        locking; at least, that is the idea.
                    //

                    var it = self.blobz_store._kv_store.iterator();

                    while (it.next()) |entry| {
                        if (entry.value_ptr._dirty_time >= entry.value_ptr._collection_time) {
                            // we "soft-spin" (with sleep) here with tryLock()
                            // and give up if it takes too long.
                            var locking_spin_count: usize = 0;
                            const is_locked: bool = blk: {
                                while (!entry.value_ptr._rw_lock.tryLock()) {
                                    locking_spin_count += 1;
                                    if (locking_spin_count >= self.locking_spin_max_count) {
                                        break :blk false;
                                    }
                                    std.time.sleep(self.locking_spin_time_ms);
                                }
                                break :blk true;
                            };

                            if (!is_locked) {
                                log.warn(
                                    "Item with key {any} could not be locked -> giving it up!",
                                    .{entry.key_ptr.*},
                                );
                                continue;
                            }

                            entry.value_ptr._collection_time = collection_time;
                            dirty_values.append(arena, .{ .key_ptr = entry.key_ptr, .wrapped_ptr = entry.value_ptr }) catch |err| {
                                log.err(
                                    "Unable to insert item with key {any} into dirty_list! {}",
                                    .{ entry.key_ptr.*, err },
                                );
                                // try later
                                break;
                            };
                        }
                    }
                }

                //
                // _insert_mutex is now unlocked. world can continue.
                //

                // now we can safely iterate over the dirty_list and "slowly"
                // persist them :-)
                // then, release their _rw_lock
                //
                // so, let's start with initializing the persistor

                const config = persist.Config.initDefault(K, self.blobz_store.dest_path);
                var persistor = persist.Persistor(K, V).init(config);
                var num_saved: usize = 0;

                for (dirty_values.items) |dirty_item| {
                    // !!!
                    // !!! unlock the item when done!
                    defer dirty_item.wrapped_ptr._rw_lock.unlock();
                    // !!!

                    persistor.persist(arena, dirty_item.key_ptr.*, dirty_item.wrapped_ptr.value) catch |err| {
                        const id = persistor.hash(dirty_item.key_ptr.*);
                        const file_path = persistor.persistor.filePath(arena, id) catch |suberr| {
                            log.err(
                                "Unable to save, unable to get path for key {any}: {}",
                                .{ dirty_item.key_ptr.*, suberr },
                            );
                            continue;
                        };
                        log.err(
                            "Unable to persist value with key {any} to {s}: {}",
                            .{ dirty_item.key_ptr.*, file_path, err },
                        );
                        continue;
                    };
                    num_saved += 1;
                }

                if (num_saved > 0) {
                    log.debug(
                        "Saved {} out of {} dirty values",
                        .{ num_saved, dirty_values.items.len },
                    );
                }

                // end of big while loop
            }
            log.debug("About to terminate", .{});
        }
    };
}

// let's test this
test SaveThread {
    const fsutils = @import("fsutils.zig");

    const alloc = std.testing.allocator;

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
    try std.fs.cwd().deleteTree(BASE_PATH);

    // the store
    var store = try blobz.Store(KEY_TYPE, Value).init(alloc, .{
        .prefix = PREFIX,
        .workdir = BASE_PATH,
        .initial_capacity = 1000,
        .save_interval_seconds = 1,
        .log_alive_message_interval_ms = 1000,
    });
    defer store.deinit(alloc);
    defer std.fs.cwd().deleteTree(BASE_PATH) catch unreachable;

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
    std.time.sleep(store.opts.save_interval_seconds * std.time.ns_per_s);
    try std.testing.expectEqual(true, fsutils.isDirPresent(BASE_PATH ++ "/" ++ PREFIX));
    try std.testing.expectEqual(false, fsutils.fileExists(BASE_PATH ++ "/" ++ PREFIX ++ "/00/01/0001.json"));
    try std.testing.expectEqual(false, fsutils.fileExists(BASE_PATH ++ "/" ++ PREFIX ++ "/00/02/0002.json"));

    // time step 2: dir exists AND first file exists
    try store.upsert(alloc, 1, value_1);

    std.time.sleep(store.opts.save_interval_seconds * std.time.ns_per_s);
    try std.testing.expectEqual(true, fsutils.isDirPresent(BASE_PATH ++ "/" ++ PREFIX));
    try std.testing.expectEqual(true, fsutils.fileExists(BASE_PATH ++ "/" ++ PREFIX ++ "/00/01/0001.json"));
    try std.testing.expectEqual(false, fsutils.fileExists(BASE_PATH ++ "/" ++ PREFIX ++ "/00/02/0002.json"));

    // time step 3: dir exists AND both files exist
    try store.upsert(alloc, 2, value_2);
    std.time.sleep(store.opts.save_interval_seconds * std.time.ns_per_s);
    try std.testing.expectEqual(true, fsutils.isDirPresent(BASE_PATH ++ "/" ++ PREFIX));
    try std.testing.expectEqual(true, fsutils.fileExists(BASE_PATH ++ "/" ++ PREFIX ++ "/00/01/0001.json"));
    try std.testing.expectEqual(true, fsutils.fileExists(BASE_PATH ++ "/" ++ PREFIX ++ "/00/02/0002.json"));

    // time step 4: update a value, and check if its file contents reflect the change
    value_2.first_name = "my";
    try store.upsert(alloc, 2, value_2);
    std.time.sleep(store.opts.save_interval_seconds * std.time.ns_per_s);
    try std.testing.expectEqual(true, fsutils.isDirPresent(BASE_PATH ++ "/" ++ PREFIX));
    try std.testing.expectEqual(true, fsutils.fileExists(BASE_PATH ++ "/" ++ PREFIX ++ "/00/01/0001.json"));
    try std.testing.expectEqual(true, fsutils.fileExists(BASE_PATH ++ "/" ++ PREFIX ++ "/00/02/0002.json"));
    var file = try std.fs.cwd().openFile(BASE_PATH ++ "/" ++ PREFIX ++ "/00/02/0002.json", .{});
    defer file.close();
    const content = try file.readToEndAlloc(alloc, 1024);
    defer alloc.free(content);
    var parsed = try std.json.parseFromSlice(Value, alloc, content, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("my", parsed.value.first_name);
    try std.testing.expectEqualStrings("mom", parsed.value.last_name);
}
