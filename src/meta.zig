const std = @import("std");

pub fn isInteger(T: type) bool {
    return @typeInfo(T) == .int;
}

pub fn isSlice(T: type) bool {
    switch (@typeInfo(T)) {
        .pointer => |ptr_info| {
            return ptr_info.size == .slice;
        },
        else => return false,
    }
}
