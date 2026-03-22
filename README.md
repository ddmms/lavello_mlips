# process_omol25

[![PyPI version](https://img.shields.io/pypi/v/process_omol25.svg)](https://pypi.org/project/process_omol25/)
[![Python versions](https://img.shields.io/pypi/pyversions/process_omol25.svg)](https://pypi.org/project/process_omol25/)
[![License](https://img.shields.io/github/license/ddmms/process_omol25.svg)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Docs Publish](https://github.com/ddmms/process_omol25/actions/workflows/gh-pages.yml/badge.svg)](https://github.com/ddmms/process_omol25/actions/workflows/gh-pages.yml)
[![PyPI Publish](https://github.com/ddmms/process_omol25/actions/workflows/publish.yml/badge.svg)](https://github.com/ddmms/process_omol25/actions/workflows/publish.yml)
[![Zenodo Publish](https://github.com/ddmms/process_omol25/actions/workflows/publish-zenodo.yml/badge.svg)](https://github.com/ddmms/process_omol25/actions/workflows/publish-zenodo.yml)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen.svg)

A Python package for processing omol-25 data using MPI.

## Installation

You can install this package locally:

```bash
uv pip install -e .
```

## Usage

This package provides three primary command-line interfaces:

### 1. Processing Data
Extract, process, and combine molecular data from an S3 bucket (or local directory):
```bash
process_omol25 --help
```
* **MPI Support**: Add `--mpi` and run via `mpirun` to distribute tasks across multiple workers natively via hybrid RMA.
* **Smart Restart**: Add `--restart` to automatically sweep the output directory, recover orphaned Parquet/XYZ pairs, and pick up right where you left off.
* **Logging**: Specify `--log-file my_log.log` to write text streams to disk (existing logs are automatically appended to, not overwritten).
* **Batch Flushing**: Use `--batch-size N` to control disk writes. If not specified, workers dynamically flush at 1% increments (with a strict minimum of 100 output structures).

### 2. Downloading Raw Data
Download original raw `orca.out` datasets from S3 without running processing logic natively on them:
```bash
download_omol25 --help
```

### 3. Verification Utility
Cross-reference a generated Parquet dataset with its respective ExtXYZ file to guarantee absolutely zero data corruption or structural mismatching:
```bash
verify_processed_omol25 --parquet props_group.parquet --extxyz structs_group.xyz
```
* This rigorously structurally aligns both tables via `geom_sha1` and flags any mathematically misassigned properties.
* Embedded timing metadata such as `process_time_s` are strictly and unconditionally excluded to prevent false-positive errors.

## License

This project is licensed under the BSD 3-Clause License - see the [LICENSE](LICENSE) file for details.
