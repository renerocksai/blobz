const std = @import("std");

pub fn build(b: *std.Build) void {
    if (!std.mem.eql(u8, @import("builtin").zig_version_string, std.mem.trim(u8, @embedFile(".zig-version"), " \r\n")))
        @panic("Use exactly Zig 0.17.0 from .zig-version");
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const module = b.addModule("blobz", .{
        .root_source_file = b.path("src/blobz.zig"),
        .target = target,
        .optimize = optimize,
    });
    const options = b.addOptions();
    options.addOption([]const u8, "contents", @embedFile("build.zig.zon"));
    module.addOptions("build.zig.zon", options);

    const library = b.addLibrary(.{
        .linkage = .static,
        .name = "blobz",
        .root_module = module,
    });
    b.installArtifact(library);
    const tests = b.addTest(.{ .root_module = module });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run persistence, ownership and saver tests");
    test_step.dependOn(&run_tests.step);

    const format = b.addFmt(.{
        .paths = b.pathList(&.{ "build.zig", "build.zig.zon", "src" }),
        .check = true,
    });
    const verify = b.step("verify", "Check formatting, compile the library and run every test");
    verify.dependOn(&format.step);
    verify.dependOn(&library.step);
    verify.dependOn(&run_tests.step);
}
