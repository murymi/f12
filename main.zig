const std = @import("std");
const fs = std.fs;
const Allocator = std.mem.Allocator;
var gpa = std.heap.GeneralPurposeAllocator(.{}){};

const FatAttr = struct { read_only: bool, hidden: bool, system: bool, volume_label: bool, subdirectory: bool, archive: bool, device: bool, other: bool, long_file_name: bool, empty: bool, free: bool };

const Geometry = struct {
    bytes_per_sector: u16,
    sectors_per_cluster: u8,
    reserved_cluster_count: u16,
    fat_count: u8,
    max_root_dir_ent: u16,
    total_sectors: u16,
    sectors_per_fat: u16,
    sectors_per_track: u16,
    head_count: u16,
    boot_signature: u8,
    volume_id: u32,
    volume_label: []const u8,
    fs_type: []const u8,
    fat: []u8,
    root_start: u32,
    data_start: u32,

    image: std.fs.File,
    allocator: Allocator,

    const Self = @This();

    //"/home/vic/Desktop/fat16/test.img"

    fn init(file_name: []const u8, allocator: Allocator) !Self {
        var geo: Geometry = undefined;
        geo.image = try fs.openFileAbsolute(file_name, .{ .mode = .read_write });
        try geo.image.seekTo(0);
        var buf = [1]u8{0} ** 72;
        var reader = std.fs.File.reader(geo.image);
        _ = try reader.readAtLeast(&buf, 72);
        geo.bytes_per_sector = @as(u16, buf[12]) << 8 | buf[11];
        geo.sectors_per_cluster = buf[13];
        geo.reserved_cluster_count = @as(u16, buf[15]) << 8 | buf[14];
        geo.fat_count = buf[16];
        geo.max_root_dir_ent = @as(u16, buf[18]) << 8 | buf[17];
        geo.total_sectors = @as(u16, buf[20]) << 8 | buf[19];
        geo.sectors_per_fat = @as(u16, buf[23]) << 8 | buf[22];
        geo.sectors_per_track = @as(u16, buf[25]) << 8 | buf[24];
        geo.head_count = @as(u16, buf[27]) << 8 | buf[26];
        geo.boot_signature = buf[38];
        geo.volume_id = std.mem.bytesToValue(u32, buf[39..43]);
        geo.volume_label = buf[43..54];
        geo.fs_type = buf[54..62];
        geo.root_start = geo.bytes_per_sector + (geo.fat_count * geo.bytes_per_sector * geo.sectors_per_fat);
        geo.data_start = geo.root_start + geo.max_root_dir_ent * 32;
        try geo.image.seekTo(geo.bytes_per_sector);
        const fat_buf = try allocator.alloc(u8, geo.sectors_per_fat * geo.bytes_per_sector);
        const r = try reader.readAtLeast(fat_buf, geo.sectors_per_fat * geo.bytes_per_sector);
        std.debug.assert(r == geo.sectors_per_fat * geo.bytes_per_sector);
        geo.allocator = allocator;
        geo.fat = fat_buf;
        return geo;
    }

    fn next_fat(self: *Self, n: u16) u16 {
        std.debug.print("index: {}\n", .{n});
        if (n % 2 == 0) {
            return (@as(u16, self.fat[1 + (3 * n) / 2] & 0b1111) << 8) | self.fat[(3 * n) / 2];
        }
        return (@as(u16, self.fat[1 + (3 * n) / 2]) << 4) | self.fat[(3 * n) / 2] >> 4;
    }

    fn update_fat(self: *Self) !void {
        try self.image.seekTo(self.bytes_per_sector);
        try self.image.writeAll(self.fat);
        try self.image.writeAll(self.fat);
    }

    fn set_file_size(self: *Self, entry_pos: u64, fsize: u32, first_cl: u16) !void {
        try self.image.seekTo(entry_pos + 26);
        try self.image.writeAll(std.mem.asBytes(&first_cl));
        try self.image.writeAll(std.mem.asBytes(&fsize));
    }

    fn set_fat(self: *Self, index: u16, value: u16) void {
        if (index % 2 == 0) {
            self.fat[1 + (3 * index) / 2] &= 0b11110000;
            self.fat[1 + (3 * index) / 2] |= (@as(u8, @intCast(value >> 8)) & 0b1111);
            self.fat[(3 * index) / 2] = @as(u8, @intCast(value & 0xff));
        } else {
            self.fat[1 + (3 * index) / 2] = @as(u8, @intCast(value >> 4));
            self.fat[(3 * index) / 2] &= 0b00001111;
            self.fat[(3 * index) / 2] |= @as(u8, @intCast(value & 0b1111)) << 4;
        }
    }

    fn cluster_pos(self: *Self, index: u16) u32 {
        return (self.data_start) + (index - 2) * (self.bytes_per_sector * self.sectors_per_cluster);
    }

    fn cluster_size(self: *Self) usize {
        return self.bytes_per_sector * self.sectors_per_cluster;
    }

    fn read_root(self: *Self) !FatDir {
        var dir = try self.read_dir(self.root_start, self.max_root_dir_ent, 0);
        dir.max_entries = self.max_root_dir_ent;
        return dir;
    }

    fn read_dir(self: *Self, dir_start: u32, max_entries: u16, fc: u16) !FatDir {
        var buf = [1]u8{0} ** 32;
        try self.image.seekTo(dir_start);
        var reader = std.fs.File.reader(self.image);
        var dir = FatDir{ .files = std.ArrayList(FatFile).init(self.allocator), .start_addr = dir_start, .max_entries = @intCast(self.cluster_size() / 32), .first_cluster = fc };
        for (0..max_entries) |i| {
            _ = try reader.readAll(&buf);
            const file = try self.parse_entry(&buf, i, dir_start + 32 * i);
            if (file.attributes.free or file.attributes.long_file_name) continue;
            try dir.files.append(file);
        }
        return dir;
    }

    fn empty_dirent(entry: []u8, name: []const u8, ext: []const u8) []u8 {
        @memset(entry, 0);
        if (ext.len == 0) {
            entry[11] |= 0x10;
        }
        const name_size = switch (name.len > 8) {
            true => 8,
            false => name.len,
        };
        for (name[0..name_size], 0..) |c, i| {
            entry[i] = c;
        }
        for (8..11) |i| {
            entry[i] = ' ';
        }
        for (ext, 8..) |c, i| {
            entry[i] = c;
        }
        return entry;
    }

    fn add_empty_dirent(self: *Self, dir_start: u16, max_entries: u16, fname: []const u8, ext: []const u8) !usize {
        var buf = [1]u8{0} ** 32;
        try self.image.seekTo(dir_start);
        var reader = std.fs.File.reader(self.image);
        for (0..max_entries) |i| {
            _ = try reader.readAll(&buf);
            const file = try self.parse_entry(&buf, i, 0);
            if (file.attributes.free or std.mem.eql(u8, &[1]u8{0} ** 32, &buf)) {
                try self.image.seekBy(-32);
                var ent: [32]u8 = undefined;
                _ = empty_dirent(&ent, fname, ext);
                try self.image.writeAll(&ent);
                return i;
            }
        }
        return error.ClusterFull;
    }

    fn parse_entry(self: *Self, entry: []u8, n: usize, addr: u64) !FatFile {
        var attr: FatAttr = std.mem.zeroes(FatAttr);

        const file_name = switch (entry[0]) {
            0x00 => block: {
                attr.empty = true;
                break :block entry[1..8];
            },
            0xe5 => block: {
                attr.free = true;
                break :block entry[1..8];
            },
            else => entry[0..8],
        };
        if (entry[11] & 0x01 > 0) attr.read_only = true;
        if (entry[11] & 0x02 > 0) attr.hidden = true;
        if (entry[11] & 0x04 > 0) attr.system = true;
        if (entry[11] & 0x08 > 0) attr.volume_label = true;
        if (entry[11] & 0x10 > 0) attr.subdirectory = true;
        if (entry[11] & 0x20 > 0) attr.archive = true;
        if (entry[11] & 0x40 > 0) attr.device = true;
        if (entry[11] & 0x80 > 0) attr.other = true;
        if (entry[11] & 0x0f > 0) attr.long_file_name = true;

        return FatFile{
            .attributes = attr,
            .create_date = "",
            .create_time = "",
            .file_name = try self.allocator.dupe(u8, std.mem.sliceTo(file_name, 0)),
            //try self.allocator.dupe(u8, file_name),
            .file_size = std.mem.bytesAsValue(u32, entry[0x1c .. 0x1c + 4]).*,
            .first_cluster = std.mem.bytesAsValue(u16, entry[0x1a..0x1c]).*,
            .next_cluster = std.mem.bytesAsValue(u16, entry[0x1a..0x1c]).*,
            .last_modified_date = "",
            .last_modified_time = "",
            .entry_number = n,
            .entry_address = addr,
        };
    }

    fn free_entry(self: *Self, addr: u16) !void {
        try self.image.seekTo(@intCast(addr));
        try self.image.writeAll(&[1]u8{0xe5});
    }

    fn fat_entries(self: *Self) u16 {
        return @intCast((self.fat.len * 8) / 12);
    }

    fn is_last_cluster(self: *Self, i: u16) bool {
        const a = self.next_fat(i);
        if (a >= 0xff8 and a <= 0xfff) {
            return true;
        }
        return false;
    }

    fn find_empty_cluster(self: *Self) !u16 {
        for (3..self.fat_entries()) |i| {
            if (self.next_fat(@intCast(i)) == 0) {
                return @intCast(i);
            }
        }
        return error.FsFull;
    }
};

const FatDir = struct {
    start_addr: u32,
    files: std.ArrayList(FatFile),
    max_entries: u16,
    first_cluster: u16,

    const Self = @This();

    fn find(self: *Self, fname: []const u8) !FatFile {
        if (fname.len > 8) @panic("long file name");
        for (self.files.items) |f| {
            if (std.mem.startsWith(u8, f.file_name, fname)) {
                return f;
            }
        }
        return error.NoSuchFile;
    }

    fn touch(self: *Self, geo: *Geometry, fname: []const u8, ext: []const u8) !void {
        if (fname.len > 8) @panic("long file name");
        if (ext.len > 3) @panic("long extension");
        try geo.image.seekTo(self.start_addr);
        _ = try geo.add_empty_dirent(self.start_addr, @intCast(geo.cluster_size() / 32), fname, ext);
    }

    fn ls(self: *Self) void {
        std.debug.print("{s:<10} {s:<5} {s:<5} {s:<5}\n", .{ "name", "size", "isdir", "cluster" });
        for (self.files.items) |file| {
            std.debug.print("{s:<10} {:<5} {:<5} {:<5}\n", .{ file.file_name, file.file_size, file.attributes.subdirectory, file.first_cluster });
        }
    }

    fn rm(self: *Self, geo: *Geometry, fname: []const u8) !void {
        if (fname.len > 8) @panic("long file name");
        if (self.find(fname)) |f| {
            try geo.free_entry(self.start_addr + @as(u16, @intCast(f.entry_number * 32)));
        } else |e| {
            return e;
        }
    }

    fn open(self: *Self, fname: []const u8) !FatFile {
        if (self.find(fname)) |f| {
            return f;
        } else |e| {
            return e;
        }
    }

    fn mkdir(self: *Self, geo: *Geometry, dirname: []const u8) !void {
        if (dirname.len > 8) @panic("long dir name");
        const idx = try geo.add_empty_dirent(self.start_addr, self.max_entries, dirname, "");
        const cluster = try geo.find_empty_cluster();
        const start = geo.cluster_pos(cluster);
        var e = try geo.add_empty_dirent(start, self.max_entries, ".", "");
        try geo.image.seekTo(start + e * 32 + 26);
        try geo.image.writeAll(std.mem.asBytes(&cluster));
        e = try geo.add_empty_dirent(start + 32, self.max_entries, "..", "");
        try geo.image.seekTo(start + e * 32 + 26);
        try geo.image.writeAll(std.mem.asBytes(&self.first_cluster));
        try geo.image.seekTo(start + 64);
        for (2..self.max_entries) |_| {
            try geo.image.writeAll(std.mem.asBytes(&@as(u32, 0)));
        }
        try geo.image.seekTo(self.start_addr + (idx * 32) + 26);
        try geo.image.writeAll(std.mem.asBytes(&cluster));
        geo.set_fat(cluster, 0xfff);
        try geo.update_fat();
    }
};

const FatFile = struct {
    file_name: []const u8,
    attributes: FatAttr,
    create_time: []const u8,
    create_date: []const u8,
    last_modified_date: []const u8,
    last_modified_time: []const u8,
    first_cluster: u16,
    entry_address: u64,
    file_size: u32,
    offset: usize = 0,
    next_cluster: u16,
    cluster_offset: usize = 0,
    entry_number: usize,

    const Self = @This();

    fn read_as_dir(self: *Self, geo: *Geometry) !FatDir {
        const cluster_addr = geo.cluster_pos(self.first_cluster);
        const max_entries = (geo.bytes_per_sector * geo.sectors_per_cluster) / 32;
        return geo.read_dir(cluster_addr, max_entries, self.first_cluster);
    }

    fn read(self: *Self, geo: *Geometry, buf: []u8) !usize {
        try geo.image.seekTo(@intCast(geo.cluster_pos(@intCast(self.next_cluster)) + self.cluster_offset));
        var reader = std.fs.File.reader(geo.image);
        while (self.offset < self.file_size) {
            const upper_bound = switch (buf.len > geo.cluster_size() - self.cluster_offset) {
                true => block: {
                    const co = self.cluster_offset;
                    self.cluster_offset = 0;
                    self.next_cluster = geo.next_fat(self.next_cluster);
                    break :block geo.cluster_size() - co;
                },
                false => buf.len,
            };
            const n = try reader.readAll(buf[0..upper_bound]);
            self.cluster_offset += n;
            self.offset += n;
            return n;
        }
        return 0;
    }

    fn last_cluster(self: *Self, geo: *Geometry) u16 {
        if (self.first_cluster == 0) {
            return 0;
        }
        var p = self.first_cluster;
        while (true) {
            if (geo.is_last_cluster(p)) return p;
            p = geo.next_fat(p);
        }
    }

    fn clusters_used(self: *Self, geo: *Geometry) u32 {
        const mod = self.file_size % geo.cluster_size();
        const a = (self.file_size) / @as(u32, @intCast(geo.cluster_size())) + switch (mod == 0) {
            true => @as(u32, 0),
            false => @as(u32, 1),
        };
        return if (a == 0) 1 else a;
    }

    fn write(self: *Self, geo: *Geometry, buf: []u8) !void {
        var current_cluster = self.last_cluster(geo);
        if (current_cluster == 0) {
            current_cluster = try geo.find_empty_cluster();
            self.first_cluster = current_cluster;
            self.cluster_offset = 0;
            geo.set_fat(current_cluster, 0xfff);
            try geo.update_fat();
        }
        self.cluster_offset = if (self.file_size > 0) self.file_size % geo.cluster_size() else 0;
        std.debug.print("rem {}, cluster size {}\n", .{ self.cluster_offset, geo.cluster_size() });
        try geo.image.seekTo(geo.cluster_pos(current_cluster) + self.cluster_offset);
        var rem = geo.cluster_size() - self.cluster_offset;
        var written: usize = 0;
        while (true) {
            if (buf.len - written > rem) {
                try geo.image.writeAll(buf[written .. written + rem]);
                written += rem;
                rem = geo.cluster_size();
                self.cluster_offset = 0;
                const new_cluster = try geo.find_empty_cluster();
                geo.set_fat(current_cluster, new_cluster);
                geo.set_fat(new_cluster, 0xfff);
                current_cluster = new_cluster;
                try geo.image.seekTo(geo.cluster_pos(current_cluster));
            } else {
                if (buf.len - written <= rem) {
                    try geo.image.writeAll(buf[written..]);
                    written += buf.len - written;
                    rem -= buf.len - written;
                    break;
                } else {
                    try geo.image.writeAll(buf[written .. written + rem]);
                    rem -= rem;
                    written += rem;
                }
            }
        }
        self.cluster_offset = rem;
        self.file_size += @intCast(written);
        try geo.set_file_size(self.entry_address, self.file_size, self.first_cluster);
        try geo.update_fat();
    }
};

const FatError = error{ NoSuchFile, FsFull, ClusterFull };

pub fn main() !void {
    const alloc = gpa.allocator();
    var geometry = try Geometry.init("/home/vic/Desktop/fat16/test.img", alloc);
    var root = try geometry.read_root();
    //var first_file = root.files.items[0];
    //var d = try first_file.read_as_dir(&geometry);

    // if(try d.touch(&geometry, "COW", "c")) {
    //     std.debug.print("touch success\n", .{});
    // } else {
    //     std.debug.print("touch failed\n", .{});
    // }
    //try d.rm(&geometry, "COW");

    //const file = try root.open("SUTI");

    //try root.touch(&geometry, "KOO", "txt");
    var file = try root.open("KOO");
    try file.write(&geometry, @constCast(&[_]u8{ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', '\n' } ** 10));
    try file.write(&geometry, @constCast(&[_]u8{ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', '\n' } ** 10));

    //var dir = try file.read_as_dir(&geometry);
    //try root.mkdir(&geometry, "KIGUNA");
    //std.debug.print("{any}\n", .{dir.fi});
    //dir.ls();
    //var my_buff = [1]u8{0} ** 64;
    // var target_file = d.files.items[4];
    // std.debug.print("{}\n", .{target_file});
    //_ = try file.read(&geometry, &my_buff);
    //std.debug.print("{s}\n", .{my_buff});
    // std.debug.print("{} {}", .{n, target_file.offset});

}


test "fat" {
    const T = std.testing;
    const alloc = gpa.allocator();
    var geo = try Geometry.init("/home/vic/Desktop/fat16/test.img", alloc);
    geo.set_fat(2, 100);
    try T.expectEqual(geo.next_fat(2), 100);

    geo.set_fat(4, 10);
    try T.expectEqual(geo.next_fat(4), 10);

    geo.set_fat(10, 4000);
    try T.expectEqual(geo.next_fat(10), 4000);

    geo.set_fat(10, 4095);
    try T.expectEqual(geo.next_fat(10), 4095);

    geo.set_fat(10, 4096);
    try T.expect(geo.next_fat(10) != 4096);

    geo.set_fat(5, 2400);
    try T.expectEqual(geo.next_fat(5), 2400);

    geo.set_fat(5, 4000);
    try T.expectEqual(geo.next_fat(5), 4000);

    geo.set_fat(5, 2050);
    try T.expectEqual(geo.next_fat(5), 2050);

    geo.set_fat(2, 2050);
    try T.expectEqual(geo.next_fat(2), 2050);

    geo.set_fat(3, 2050);
    try T.expectEqual(geo.next_fat(3), 2050);

    geo.set_fat(4, 2050);
    try T.expectEqual(geo.next_fat(4), 2050);

    geo.set_fat(5, 2050);
    try T.expectEqual(geo.next_fat(5), 2050);

    geo.set_fat(5, 0);
    try T.expectEqual(geo.next_fat(5), 0);
}
