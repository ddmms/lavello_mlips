# process_omol25

A Python package for processing omol-25 data using MPI.

## Installation

You can install this package locally:

```bash
pip install -e .
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
