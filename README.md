# xsz - A Multi-threaded Btrfs Compression Analysis Tool

This is a rewrite of `compsize`, but faster, and use less memory.

## Introduction

`xsz` is an open-source tool designed to measure the used compression types and effective compression ratios of files on a Btrfs filesystem,
producing a comprehensive report. It is inspired by [compsize](https://github.com/kilobyte/compsize) and maintains compatibility with its command-line interface.
However, `xsz` goes a step further by leveraging multi-threading to significantly enhance performance,
especially on NVMe SSDs, while also providing notable speed improvements on other hardware.

## Usage

`xsz` follows the same command-line syntax as `compsize`. To get started, simply run:

```sh

xsz /path/to/dir
```

For more detailed usage instructions and options, refer to the help message:

```console
xsz --help
Usage: xsz [options] file-or-dir1 [file-or-dir2 ...]

xsz displays total space used by set of files, taking into account
compression, reflinks, partially overwritten extents.

Options:
    -h, --help              print this help message and exit
    -b, --bytes             display raw bytes instead of human-readable sizes
    -x, --one-file-system   don't cross filesystem boundaries
    -j N, --jobs=N          allow N jobs at once
```

## Important Notes

This project has not undergone rigorous testing. Use it in production environments at your own risk.
If you encounter any issues or have suggestions for improvement, please feel free to open an issue or join the discussion.

## Contribution

We warmly welcome contributions from the community! Areas where your help would be particularly valuable include:

  - Improving error handling.

  - Adding benchmarks.

  - Enhancing performance.

  - Improving code readability.

We're looking forward to your contribution and feedback.

## FAQ

Q: Why you write `xsz`?

A: Because `compsize` is so useful for me, but sometimes too slow. I had a try to speed it up, and `xsz` is the result.
   I'm glad that I can get a more than 4x speed up by using `xsz`.

Q: Why rust?

A: Because rust makes me, who is too stupid to write correct multi-thread programs in C,
   able to write a working multi-thread program. You know, multi-threading is important when you want to make
   some cpu bounded tasks faster.

Q: But is `compsize` really a cpu bounded task? Does multi-threading really work in this case?

A: Yes, and no. The task is actually at somewhere between cpu bounded and io bounded.
   If you run `compsize` on a fast enough disk, let's say, NVMe SSD, then you'll
   find that the cpu usage can be more than 50%, and reaching above 90%, while the load of SSD is quite low.
   So, yes, it is kind of a cpu bounded task. But if you run `compsize` on a HDD,
   you'll find that most time is spent on waiting the disk. So it's more like a io bounded task.

Q: So does `xsz` also run faster on HDD?

A: Yes! I think it's because we do a lot less open and close syscalls.
   So when running in `-j1`, `xsz` is a lot faster than `compsize`.
   But multi-threading hardly improve the performance in this case.

Q: Why `xsz` is faster?

A: TL;DR: Because we use multi-threading, and do less syscall.

   Longer version:
   
   - we reduce a lot of unnecessary open and close syscalls;
   
   - we use multi-threading to call ioctl, which costs most time;
   
   - we use multi-threading to walkdir because when cache is hot, a single-thread walkdir can be the bottleneck.

   Full version: I'm too lazy to finish this part now...

Q: Oh, yes, `xsz` seems good. Can I use it as a dropin replacement for `compsize`?

A: Yes, and no. We try to have the same cli-arguments with `compsize`, but we added a `-j`
   so you can set the number of worker threads. And, `xsz` is not widely used and tested like
   `compsize`. So welcome to have a try. And if you find the result is different from `compsize`,
   please report it as a BUG.

Q: How many worker threads should I set on my machine?

A: I don't know. It depends on your cpu and disk. You can try to increase worker threads until
   either your cpu or your disk is under 100% load.

## Benchmark

On a SATA SSD device, with some snapshots for backing up, and some large media files.

Cache is cleared before each run.

```console
$ sudo time ./xsz -j1 /mnt/1
Processed 5663444 files, 1097233 regular extents (3859780 refs), 3459956 inline.
Type       Perc     Disk Usage   Uncompressed Referenced
TOTAL       95%      827G         865G         1.3T
none       100%      811G         811G         1.1T
zstd        29%       15G          54G         192G
4.27user 37.46system 1:14.13elapsed 56%CPU (0avgtext+0avgdata 31420maxresident)k
5060832inputs+0outputs (0major+46434minor)pagefaults 0swaps

$ sudo time ./xsz -j4 /mnt/1
Processed 5663444 files, 1097233 regular extents (3859780 refs), 3459956 inline.
Type       Perc     Disk Usage   Uncompressed Referenced
TOTAL       95%      827G         865G         1.3T
none       100%      811G         811G         1.1T
zstd        29%       15G          54G         192G
4.89user 37.88system 0:20.42elapsed 209%CPU (0avgtext+0avgdata 35820maxresident)k
5060832inputs+0outputs (0major+79430minor)pagefaults 0swaps

$ sudo time compsize /mnt/1
Processed 5663444 files, 1097233 regular extents (3859780 refs), 3459956 inline.
Type       Perc     Disk Usage   Uncompressed Referenced
TOTAL       95%      827G         865G         1.3T
none       100%      811G         811G         1.1T
zstd        29%       15G          54G         192G
3.72user 72.98system 1:50.07elapsed 69%CPU (0avgtext+0avgdata 80132maxresident)k
5254008inputs+0outputs (1major+24967minor)pagefaults 0swaps
```

On a HDD device, with a lot of small files, and some program files.

Cache is cleared before each run.

```console
$ sudo time ./xsz -j1 /mnt/guest
Processed 393486 files, 215207 regular extents (215207 refs), 178308 inline.
Type       Perc     Disk Usage   Uncompressed Referenced
TOTAL      100%       29G          29G          29G
none       100%       29G          29G          29G
0.37user 3.47system 1:07.54elapsed 5%CPU (0avgtext+0avgdata 5436maxresident)k
830912inputs+0outputs (0major+7825minor)pagefaults 0swaps

$ sudo time ./xsz -j4 /mnt/guest
Processed 393486 files, 215207 regular extents (215207 refs), 178308 inline.
Type       Perc     Disk Usage   Uncompressed Referenced
TOTAL      100%       29G          29G          29G
none       100%       29G          29G          29G
0.40user 3.97system 0:57.72elapsed 7%CPU (0avgtext+0avgdata 8064maxresident)k
830912inputs+0outputs (0major+5093minor)pagefaults 0swaps

$ sudo time compsize /mnt/guest
Processed 393486 files, 215207 regular extents (215207 refs), 178308 inline.
Type       Perc     Disk Usage   Uncompressed Referenced
TOTAL      100%       29G          29G          29G
none       100%       29G          29G          29G
0.42user 9.45system 2:49.78elapsed 5%CPU (0avgtext+0avgdata 12648maxresident)k
954080inputs+0outputs (0major+2990minor)pagefaults 0swaps
```
