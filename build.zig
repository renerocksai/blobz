const std = @import("std");

const build_zig_zon = @embedFile("build.zig.zon");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    // we export this module
    const lib_mod = b.addModule("blobz", .{
        .root_source_file = b.path("src/blobz.zig"),
        .target = target,
        .optimize = optimize,
    });

    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "blobz",
        .root_module = lib_mod,
    });

    b.installArtifact(lib);

    // Make build.zig.zon accessible in module
    var my_options = std.Build.Step.Options.create(b);
    my_options.addOption([]const u8, "contents", build_zig_zon);
    lib.root_module.addOptions("build.zig.zon", my_options);

    // const exe_mod = b.createModule(.{
    //     .root_source_file = b.path("examples/blah/main.zig"),
    //     .target = target,
    //     .optimize = optimize,
    // });
    //
    // exe_mod.addImport("blobz", lib_mod);
    // const exe = b.addExecutable(.{
    //     .name = "blobz",
    //     .root_module = exe_mod,
    // });
    //
    // b.installArtifact(exe);
    //
    // const run_cmd = b.addRunArtifact(exe);
    //
    // run_cmd.step.dependOn(b.getInstallStep());
    //
    // if (b.args) |args| {
    //     run_cmd.addArgs(args);
    // }
    //
    // const run_step = b.step("run", "Run the app");
    // run_step.dependOn(&run_cmd.step);

    const lib_unit_tests = b.addTest(.{
        .root_module = lib_mod,
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
}
