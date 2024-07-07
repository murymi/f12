const std = @import("std");
const fs = std.fs;
const Allocator = std.mem.Allocator;
var gpa = std.heap.GeneralPurposeAllocator(.{}){};

const FatFile = struct {
    file_name: []const u8,
    attributes: FatAttr,
    create_time: []const u8,
    create_date: []const u8,
    last_modified_date: []const u8,
    last_modified_time: []const u8,
    first_cluster: u16,
    file_size: u32,
    offset: usize = 0,
    next_cluster: u16,
    cluster_offset: usize = 0,

    const Self = @This();

    fn read_as_dir(self: *Self, geo: *Geometry) !FatDir {
        const cluster_addr = geo.cluster_pos(self.first_cluster);
        const max_entries = (geo.bytes_per_sector * geo.sectors_per_cluster)/32;
        return geo.read_dir(cluster_addr, max_entries);
    }

    fn read(self:*Self, geo: *Geometry, buf: []u8) !usize {
        try geo.image.seekTo(@intCast(geo.cluster_pos(@intCast(self.next_cluster)) + self.cluster_offset));
        var reader = std.fs.File.reader(geo.image);
        while(self.offset < self.file_size) {
            const upper_bound = switch (buf.len > geo.cluster_size() - self.cluster_offset) {
                true => block:{
                    const co = self.cluster_offset;
                    self.cluster_offset = 0;
                    self.next_cluster = geo.next_fat(self.next_cluster);
                    break: block geo.cluster_size() - co;
                },
                false => buf.len
            };
            const n = try reader.readAll(buf[0..upper_bound]);
            self.cluster_offset += n;
            self.offset += n;
            return n;
        }
        return 0;
    }
};

const FatAttr = struct {
    read_only: bool,
    hidden: bool,
    system: bool,
    volume_label: bool,
    subdirectory: bool,
    archive: bool,
    device: bool,
    other: bool,
    long_file_name: bool,
    empty: bool,
    free: bool,
};

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
    root_start: u16,
    data_start: u16,

    image: std.fs.File,
    allocator: Allocator,

    const Self = @This();

    //"/home/vic/Desktop/fat16/test.img"

    fn init(file_name: []const u8, allocator: Allocator) !Self {
        var geo: Geometry = undefined;
        geo.image = try fs.openFileAbsolute(file_name, .{ .mode = .read_only });
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
        if (n % 2 == 0) {
            return (@as(u16, self.fat[1 + (3 * n) / 2] & 0b1111) << 8) | self.fat[(3 * n) / 2];
        }
        return self.fat[1 + (3 * n) / 2] << 4 | @as(u16, self.fat[(3 * n) / 2] >> 4);
    }

    fn cluster_pos(self: *Self, index: u16) u16 {
        return (self.data_start) + (index - 2) * (self.bytes_per_sector * self.sectors_per_cluster);
    }

    fn cluster_size(self: *Self) usize {
        return self.bytes_per_sector * self.sectors_per_cluster;
    }

    fn read_root(self: *Self) !FatDir {
        return self.read_dir(self.root_start, self.max_root_dir_ent);
    }

    fn read_dir(self: *Self, dir_start: u16, max_entries: u16) !FatDir {
        var buf = [1]u8{0} ** 32;
        try self.image.seekTo(dir_start);
        var reader = std.fs.File.reader(self.image);
        var dir = FatDir{ .files = std.ArrayList(FatFile).init(self.allocator) };
        for (0..max_entries) |_| {
            _ = try reader.readAll(&buf);
            const file = try self.parse_entry(&buf);
            if (file.attributes.empty or file.attributes.long_file_name) continue;
            try dir.files.append(file);
        }
        return dir;
    }

    fn parse_entry(self: *Self, entry: []u8) !FatFile {
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
            .file_name = try self.allocator.dupe(u8, file_name),
            //try self.allocator.dupe(u8, std.mem.sliceTo(file_name, 0)),
            .file_size = std.mem.bytesAsValue(u32, entry[0x1c .. 0x1c + 4]).*,
            .first_cluster = std.mem.bytesAsValue(u16, entry[0x1a..0x1c]).*,
            .next_cluster = std.mem.bytesAsValue(u16, entry[0x1a .. 0x1c]).*,
            .last_modified_date = "",
            .last_modified_time = "",
        };
    }
};

const FatDir = struct {
    files: std.ArrayList(FatFile),
};

pub fn main() !void {
    const alloc = gpa.allocator();
    var geometry = try Geometry.init("/home/vic/Desktop/fat16/test.img", alloc);
    const root = try geometry.read_root();
    var first_file = root.files.items[0];
    const d = try first_file.read_as_dir(&geometry);
    var target_file = d.files.items[4];
    std.debug.print("{}\n", .{target_file});
    var my_buff = [1]u8{0} ** 64;
    const n = try target_file.read(&geometry, &my_buff);
    std.debug.print("{s}\n", .{my_buff});
    std.debug.print("{} {}", .{n, target_file.offset});

}
