//! filesystem utils used in tests

const std = @import("std");

pub fn fileExists(file: []const u8) bool {
    _ = std.fs.cwd().statFile(file) catch return false;
    return true;
}

pub fn isDirPresent(dirname: []const u8) bool {
    var dir: ?std.fs.Dir = std.fs.cwd().openDir(dirname, .{}) catch null;
    if (dir) |*d| {
        defer d.close();
        return true;
    }
    return false;
}
