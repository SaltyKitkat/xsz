# xsz - A Multi-threaded Btrfs Compression Analysis Tool

## Introduction

xsz is an open-source tool designed to measure the used compression types and effective compression ratios of files on a Btrfs filesystem,
producing a comprehensive report. It is inspired by [compsize](https://github.com/kilobyte/compsize) and maintains compatibility with its command-line interface.
However, xsz goes a step further by leveraging multi-threading to significantly enhance performance,
especially on NVMe SSDs, while also providing notable speed improvements on other hardware.

## Features

- Multi-threading: Accelerates the analysis process, offering substantial performance gains, particularly on NVMe SSDs.

- Command-line Compatibility: Fully compatible with compsize commands, ensuring a seamless transition for existing users.

## Usage

xsz follows the same command-line syntax as compsize. To get started, simply run:

```bash

xsz /path/to/dir
```

For more detailed usage instructions and options, refer to the Documentation.

## Important Notes

This project has not undergone rigorous testing. Use it in production environments at your own risk.
If you encounter any issues or have suggestions for improvement, please feel free to open an issue or join the discussion.

## Contribution

We warmly welcome contributions from the community! Areas where your help would be particularly valuable include:

  - Improving error handling.

  - Adding benchmarks.

  - Enhancing performance.

  - Improving code readability.

Thank you for your interest in xsz! We look forward to your contributions and feedback.
