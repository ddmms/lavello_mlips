import argparse
import logging
from pathlib import Path

from .process_omol25 import S3DataProcessor, setup_logging


def parse_args():
    """Parses command-line arguments for the script."""
    parser = argparse.ArgumentParser(
        description="Download, process, and combine molecular data from an S3 bucket."
    )
    parser.add_argument(
        "--login-file",
        type=Path,
        required=False,
        default=None,
        help="Path to the JSON file containing S3 access_key and secret_key.",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="omol-25-small",
        help="Name of the S3 bucket to download data from.",
    )
    parser.add_argument(
        "--data-source",
        type=Path,
        default=Path("./train_4M/"),
        help="Path to the json file to process",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("."),
        help="Dir path to write the processed properties.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=-1,
        help="Default number of samples to process if --end-index is not specified.",
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="The starting index (inclusive) of the dataset to process.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level.",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Optional path to a file where logs will also be written.",
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Skip configurations marked as processed in --data-source")
    parser.add_argument(
        "--memory-threshold-gb",
        type=float,
        default=2.0,
        help="Memory threshold in GB to flush records to disk.")
    parser.add_argument(
        "--local-dir",
        type=Path,
        default=None,
        help="Optional local directory to read data from instead of S3.")
    parser.add_argument(
        "--mpi",
        action="store_true",
        help="Run using MPI.")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.mpi:
        from mpi4py import MPI as mpi
        comm = mpi.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()
    else:
        comm = None
        rank = 0
        size = 1

    log_level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    if rank == 0 :
        logfile = args.log_file if args.log_file else Path(args.data_source.stem + ".log")
        setup_logging(
            level=log_level_map.get(args.log_level.upper(), logging.INFO),
            log_file_path=logfile
        )

    if not args.local_dir and not args.login_file:
        raise ValueError("--login-file is required when --local-dir is not specified.")

    processor = S3DataProcessor(args, rank, size, comm)
    processor.run_mpi()


if __name__ == "__main__":
    main()
