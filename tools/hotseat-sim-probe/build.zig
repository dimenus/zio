const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Sim on and pinned scheduling are declared by the probe's root module
    // (`zio_options` in src/main.zig), the way a second user would declare them.
    const zio = b.dependency("zio", .{ .target = target, .optimize = optimize });

    const mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .imports = &.{.{ .name = "zio", .module = zio.module("zio") }},
    });
    // Stands in for hotseat's patched ENet clock (src/enet_patch/monotonic_time.c).
    mod.addCSourceFile(.{ .file = b.path("src/foreign_clock.c") });

    const exe = b.addExecutable(.{ .name = "hotseat-sim-probe", .root_module = mod });
    b.installArtifact(exe);

    const run = b.addRunArtifact(exe);
    run.addPassthruArgs();
    run.has_side_effects = true;
    b.step("run", "Run one probe: zig build run -- <probe> [seed]").dependOn(&run.step);
}
