//! Trade-Off:
//! If you use numeric, ascending keys, order is preserved in the file system.
//! If you use arbitrary keys, they might fit your use-case better but they'll
//! need to be hashed --> order will not be preserved in the hashed keys, in
//! the file system.

const std = @import("std");
const meta = @import("meta.zig");
const log = std.log.scoped(.persistor);

const Allocator = std.mem.Allocator;

pub var max_file_size: usize = 1024 * 1024;

/// Configuration structure. Users may override shardLevels and bitsPerLevel.
pub const Config = struct {
    /// How many subdirectory levels to create.
    shard_levels: u8,
    /// How many bits to use per level (must be a multiple of 4). Default: 8 bits (2 hex digits).
    bits_per_level: u8 = 8,
    /// Base directory for persisting files.
    base_path: []const u8,

    /// Use this init function if you don't care about the bits_per_level and
    /// the shard_levels.
    pub fn initDefault(ID: type, base_path: []const u8) Config {
        return .{
            .shard_levels = shard_levels_default(ID),
            .base_path = base_path,
        };
    }

    /// Default shard levels based on the ID type.
    /// These defaults can be tuned; here we choose 3 for u64 and 4 for u128.
    pub fn shard_levels_default(ID: type) u8 {
        return switch (@bitSizeOf(ID)) {
            64 => 3,
            128 => 4,
            else => 2,
        };
    }
};

/// NumericKeyPersistor forms the core of our implementation. It uses a numeric
/// ID type. If you need arbitrary IDs, use the generic Persistor (further
/// down) instead.
pub fn NumericKeyPersistor(comptime ID: type, Value: type) type {
    // Assume ID is an unsigned integer.
    comptime {
        if (!meta.isInteger(ID)) {
            @compileError("ID must be an integer type");
        }
    }

    return struct {
        // Compute the number of hex digits from the bit-size.
        pub const bit_size = @bitSizeOf(ID);
        pub const num_hex_digits = bit_size / 4;

        pub const Self = @This();

        config: Config,

        /// Initialize the persistor with a given config.
        pub fn init(config: Config) Self {
            return .{
                .config = config,
            };
        }

        /// Compute the file path for a given ID.
        /// The ID is formatted as a zero-padded hex string with fixed width.
        pub fn filePath(self: *Self, gpa: Allocator, id: ID) ![]const u8 {
            // Create a fixed buffer to hold the hex representation.
            var key_buf: [num_hex_digits]u8 = undefined;
            const format_string = std.fmt.comptimePrint("{{x:0>{d}}}", .{num_hex_digits});
            // std.debug.print(
            //     "format_string for {s} {d} digits: {s}",
            //     .{ @typeName(ID), num_hex_digits, format_string },
            // );
            const key_hex = try std.fmt.bufPrint(&key_buf, format_string, .{id});

            // Each level uses (bits_per_level / 4) hex digits.
            const digits_per_level = self.config.bits_per_level / 4;
            const prefix_hex_len = self.config.shard_levels * digits_per_level;
            if (prefix_hex_len > key_hex.len) {
                return error.InvalidShardingConfig;
            }

            // Build an array of path segments:
            // First: the basePath.
            // Then: one segment per shard level.
            // Finally: the filename, composed of the remaining hex digits plus ".json".
            const seg_count: usize = self.config.shard_levels + 2;
            var segments = try gpa.alloc([]const u8, seg_count);
            segments[0] = self.config.base_path;

            // For each shard level, use the corresponding slice of key_hex.
            for (0..self.config.shard_levels) |i| {
                segments[i + 1] = key_hex[(i * digits_per_level)..((i + 1) * digits_per_level)];
            }

            // Note: We always use the entire hex representation of the key as
            //       filename. That makes it easier to iterate over directories
            //       and collect key/value pairs, as individual files contain
            //       all the information (key=filename, value=content).
            //
            // Alternative: Take the remainder after the digits we used for
            //              segments as the basename for the file in the leaf
            //              directory.
            // const filename_hex = key_hex[prefix_hex_len..key_hex.len];
            const filename_hex = key_hex;
            const ext = ".json";

            const filename = try std.fmt.allocPrint(gpa, "{s}{s}", .{ filename_hex, ext });
            defer gpa.free(filename);
            segments[self.config.shard_levels + 1] = filename;

            // Join segments into a complete path.
            const fullPath = try std.fs.path.join(gpa, segments);
            gpa.free(segments);
            return fullPath; // already duped by std.fs.path.join
        }

        /// Persist the given value (which must be JSON-serializable) to its file.
        /// This function computes the file path based on the key, ensures the directory exists,
        /// serializes the value into JSON, and writes it to disk.
        pub fn persist(self: *Self, gpa: Allocator, id: ID, value: anytype) !void {
            const path = try self.filePath(gpa, id);
            defer gpa.free(path);

            const dirPath = std.fs.path.dirname(path) orelse return error.NoDirName;
            std.fs.cwd().makePath(dirPath) catch |err| {
                if (err != error.PathAlreadyExists) return err;
            };

            const json_str = try std.json.stringifyAlloc(gpa, value, .{});

            // TODO: maybe use a buffered writer here
            const file = try std.fs.cwd().createFile(path, .{ .truncate = true });
            defer file.close();
            try file.writeAll(json_str);
            gpa.free(json_str);
        }

        pub fn load(self: *Self, arena: Allocator, id: ID) !Value {
            const path = try self.filePath(arena, id);
            return self.loadFromPath(arena, path);
        }

        pub fn loadFromPath(_: *Self, arena: Allocator, path: []const u8) !Value {
            // TODO: maybe use BufferedReader

            var file = try std.fs.cwd().openFile(path, .{});
            defer file.close();

            const json_str = try file.readToEndAlloc(arena, max_file_size);

            const ret = std.json.parseFromSliceLeaky(Value, arena, json_str, .{ .allocate = .alloc_always });
            return ret;
        }
    };
}

pub fn Hasher(K: type, ID: type) type {
    return struct {
        hash: fn (K) ID,

        pub const default: @This() = .{
            .hash = xxHash64,
        };

        pub const hashing: @This() = .{ .hash = xxHash64 };
        pub const identity: @This() = .{ .hash = identityHash };

        /// xx hash
        pub fn xxHash64(key: K) ID {
            return std.hash.XxHash64.hash(0x12345678, key);
        }

        /// identity hash for integer types
        pub fn identityHash(key: K) K {
            return key;
        }
    };
}

/// The generic persistor:
/// Works out if key is numeric -> no key hashing is performed, key order is
/// preserved in the filesystem.
/// If key is NOT numeric, its hash is used --> order is not preserved!
pub fn Persistor(comptime K: type, V: type) type {
    comptime {
        if (!meta.isInteger(K) and !meta.isSliceOf(K, u8)) {
            @compileError("Persistor<K> only supports integer or []const u8 keys");
            // TODO: check if slice is of type []const u8
        }
    }

    // Decide ID type and hash function at compile time:

    // Now wrap the NumericKeyPersistor:
    return struct {
        const is_int = meta.isInteger(K);
        const ID = if (is_int) K else u64;
        const hasher: Hasher(K, ID) = if (is_int) .identity else .hashing;

        pub const KV = struct { key: K, value: V };
        const DiskPayload = if (is_int) V else KV;

        const Self = @This();
        const NumIdPersistor = NumericKeyPersistor(ID, DiskPayload);

        persistor: NumIdPersistor,

        pub fn init(config: Config) Self {
            return .{ .persistor = NumIdPersistor.init(config) };
        }

        pub fn persist(self: *Self, gpa: std.mem.Allocator, key: K, value: V) !void {
            const id: ID = hasher.hash(key);
            const payload: DiskPayload = if (is_int) value else .{ .key = key, .value = value };
            try self.persistor.persist(gpa, id, payload);
        }

        pub fn hash(_: *Self, key: K) ID {
            return hasher.hash(key);
        }

        /// You know the key, so load and return only the value
        pub fn load(self: *Self, arena: std.mem.Allocator, key: K) !V {
            const id: ID = hasher.hash(key);
            const obj = try self.persistor.load(arena, id);
            if (is_int) return obj else {

                // key comparison of u8 slices
                if (!std.mem.eql(u8, obj.key, key)) {
                    return error.HashCollision;
                }
                return obj.value;
            }
        }

        /// You only know the filename, so return the key and the value
        pub fn loadFromPath(self: *Self, arena: Allocator, path: []const u8) !KV {
            const obj = try self.persistor.loadFromPath(arena, path);
            if (is_int) {
                return .{
                    .key = try std.fmt.parseInt(K, std.fs.path.basename(path), 16),
                    .value = obj,
                };
            } else {
                return obj;
            }
        }
    };
}

test Persistor {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const KEY_TYPE = u16;
    const BASE_PATH = ",,test_persistor";

    const config = Config.initDefault(KEY_TYPE, BASE_PATH);

    const Value = struct {
        first_name: []const u8,
        last_name: []const u8,

        pub fn deinit(self: *const @This(), allocator: Allocator) void {
            allocator.free(self.first_name);
            allocator.free(self.last_name);
        }
    };

    const value_1: Value = .{ .first_name = "rene", .last_name = "rocksai" };
    const value_2: Value = .{ .first_name = "your", .last_name = "mom" };
    var persistor = Persistor(KEY_TYPE, Value).init(config);
    try persistor.persist(gpa, 1, value_1);
    const file_1_path = try persistor.persistor.filePath(gpa, 1);
    defer gpa.free(file_1_path);
    try std.testing.expectEqualStrings(BASE_PATH ++ "/00/01/0001.json", file_1_path);

    try persistor.persist(gpa, 2, value_2);
    const file_2_path = try persistor.persistor.filePath(gpa, 2);
    defer gpa.free(file_2_path);
    try std.testing.expectEqualStrings(BASE_PATH ++ "/00/02/0002.json", file_2_path);

    const read_value_1 = try persistor.load(arena, 1);
    defer read_value_1.deinit(arena);
    try std.testing.expectEqualStrings(value_1.first_name, read_value_1.first_name);
    try std.testing.expectEqualStrings(value_1.last_name, read_value_1.last_name);

    const read_value_2 = try persistor.load(arena, 2);
    defer read_value_2.deinit(arena);
    try std.testing.expectEqualStrings(value_2.first_name, read_value_2.first_name);
    try std.testing.expectEqualStrings(value_2.last_name, read_value_2.last_name);

    try std.fs.cwd().deleteTree(BASE_PATH);
}

test "Hashing Persistor" {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const KEY_TYPE = []const u8;
    const BASE_PATH = ",,test_hashing_persistor";

    const config = Config.initDefault(KEY_TYPE, BASE_PATH);

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

    var persistor = Persistor(KEY_TYPE, Value).init(config);

    // value 1
    {
        const value_1: Value = .{ .my_key_field = "user 1", .first_name = "rene", .last_name = "rocksai" };

        try persistor.persist(gpa, value_1.my_key_field, value_1);

        const file_1_hash = persistor.hash(value_1.my_key_field);
        const file_1_hash_str = try std.fmt.allocPrint(gpa, "{x:0}", .{file_1_hash});
        defer gpa.free(file_1_hash_str);
        try std.testing.expectEqualStrings("31473c89de0732bc", file_1_hash_str);

        const file_1_path = try persistor.persistor.filePath(gpa, file_1_hash);
        defer gpa.free(file_1_path);
        try std.testing.expectEqualStrings(BASE_PATH ++ "/31/47/3c/89/31473c89de0732bc.json", file_1_path);

        const read_value_1 = try persistor.load(arena, value_1.my_key_field);
        defer read_value_1.deinit(arena);
        try std.testing.expectEqualStrings(value_1.first_name, read_value_1.first_name);
        try std.testing.expectEqualStrings(value_1.last_name, read_value_1.last_name);
    }

    // value 2
    {
        const value_2: Value = .{ .my_key_field = "user 2", .first_name = "your", .last_name = "mom" };
        try persistor.persist(gpa, value_2.my_key_field, value_2);
        const file_2_hash = persistor.hash(value_2.my_key_field);
        const file_2_hash_str = try std.fmt.allocPrint(gpa, "{x:0}", .{file_2_hash});
        defer gpa.free(file_2_hash_str);
        try std.testing.expectEqualStrings("8257bdc0e590ad97", file_2_hash_str);

        const file_2_path = try persistor.persistor.filePath(gpa, file_2_hash);
        defer gpa.free(file_2_path);
        try std.testing.expectEqualStrings(BASE_PATH ++ "/82/57/bd/c0/8257bdc0e590ad97.json", file_2_path);

        const read_value_2 = try persistor.load(arena, value_2.my_key_field);
        defer read_value_2.deinit(arena);
        try std.testing.expectEqualStrings(value_2.first_name, read_value_2.first_name);
        try std.testing.expectEqualStrings(value_2.last_name, read_value_2.last_name);
    }

    try std.fs.cwd().deleteTree(BASE_PATH);
}
