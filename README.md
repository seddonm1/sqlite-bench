# sqlite-bench

A project to test SQLite Transaction behavior.

Code to accompany blog post: https://reorchestrate.com/posts/sqlite-transactions

## How to use

Compile by running `cargo build --release`.

Run like: `cargo run --release -- --help`:

```bash
Benchmarking SQLite

Usage: sqlite-bench [OPTIONS] --path <PATH> --output <OUTPUT>

Options:
  -p, --path <PATH>           Path to the SQLite file
  -o, --output <OUTPUT>       Path to the output result file
  -s, --seed <SEED>           Number of records to seed the into the table [default: 1000000]
  -t, --threads <THREADS>...  Number of concurrent threads to spawn [default: 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16]
  -s, --scans <SCANS>...      Scan operations to perform per transaction [default: 0 10]
  -u, --updates <UPDATES>...  Update operations to perform per transaction [default: 0 1 10]
  -h, --help                  Print help
  -V, --version               Print version
```

It is a good idea to run this against an in-memory filesystem first to protect your solid-state-drive.

MacOS:

```bash
diskutil erasevolume apfs 'ramdisk' `hdiutil attach -nobrowse -nomount ram://33554432`
```

Linux:

```bash
sudo mkdir -p /mnt/ramdisk
sudo mount -t tmpfs -o size=16g tmpfs /mnt/ramdisk
```

A multiplatform Docker image is available at: https://github.com/users/seddonm1/packages/container/package/sqlite-bench