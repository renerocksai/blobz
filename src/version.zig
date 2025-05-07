const std = @import("std");
const build_zig_zon = @import("build.zig.zon");

pub fn print_version() void {
    std.debug.print(
        "version is `{s}`",
        .{version() orelse "(unknown version)"},
    );
}

pub fn version() ?[]const u8 {
    var it = std.mem.splitScalar(
        u8,
        build_zig_zon.contents,
        '\n',
    );
    while (it.next()) |line_untrimmed| {
        const line = std.mem.trim(u8, line_untrimmed, " \t\n\r");
        if (std.mem.startsWith(u8, line, ".version")) {
            var tokenizer = std.mem.tokenizeAny(
                u8,
                line[".version".len..],
                " \"=",
            );
            return tokenizer.next();
        }
    }
    return null;
}
