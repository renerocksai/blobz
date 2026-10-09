//! filesystem utils used in tests

const std = @import("std");

pub fn fileExists(io: std.Io, file: []const u8) bool {
    _ = std.Io.Dir.cwd().statFile(io, file, .{}) catch return false;
    return true;
}

pub fn isDirPresent(io: std.Io, dirname: []const u8) bool {
    var dir: ?std.Io.Dir = std.Io.Dir.cwd().openDir(io, dirname, .{}) catch null;
    if (dir) |*d| {
        defer d.close(io);
        return true;
    }
    return false;
}
