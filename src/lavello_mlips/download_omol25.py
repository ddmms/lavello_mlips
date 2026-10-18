import argparse
import logging
import os
import re
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
from io import BytesIO
import tarfile
from tarfile import open as tar_open
from zstandard import ZstdDecompressor
import boto3
from botocore.config import Config
from tqdm import tqdm
import tempfile
import time

from .utils import setup_logging, json_load, json_dump, extract_gbw_bytes, get_shm_temp_dir
from mpi4py import MPI

_shm_temp = get_shm_temp_dir()
if _shm_temp:
    tempfile.tempdir = _shm_temp

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download and extract molecular data asynchronously via Manager/Worker."
    )
    parser.add_argument(
        "--login-file",
        type=Path,
        required=False,
        default=None,
        help="Path to credentials JSON.",
    )
    parser.add_argument(
        "--bucket", type=str, default="omol-25-small", help="S3 bucket."
    )
    parser.add_argument(
        "--data-source", type=Path, required=True, help="JSON file with keys."
    )
    parser.add_argument(
        "--sample-size", type=int, default=-1, help="Number of samples to run."
    )
    parser.add_argument("--start-index", type=int, default=None, help="Start index.")
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Skip configurations marked as processed.",
    )
    parser.add_argument(
        "--local-dir",
        type=Path,
        default=None,
        help="Optional local directory to read data from instead of S3.",
    )
    parser.add_argument("--mpi", action="store_true", help="Run with MPI.")
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
        "--tmp-dir",
        "--temp-dir",
        dest="tmp_dir",
        type=Path,
        default=Path(os.environ.get("LAVELLO_TMPDIR", ".")),
        help="Optional custom scratch/temporary directory for temporary files (defaults to current directory or LAVELLO_TMPDIR).",
    )
    return parser.parse_args()


def process_prefix(x: str, data: Dict[str, Any], args: argparse.Namespace, s3_client: Any) -> Optional[str]:
    """Processes a single prefix synchronously."""
    raw_key = data[x]["key"]
    key_list = [raw_key] if isinstance(raw_key, str) else raw_key

    success = True
    for k in key_list:
        source = x + k
        try:
            buffer = BytesIO()
            if args.local_dir:
                local_path = args.local_dir / source
                if not local_path.exists():
                    logger.warning(f"Local file '{local_path}' not found. Skipping.")
                    success = False
                    continue
                content = local_path.read_bytes()
                buffer.write(content)
            else:
                response = s3_client.get_object(Bucket=args.bucket, Key=source)
                buffer.write(response["Body"].read())

            buffer.seek(0)
            extract_buffer(buffer, x, k)

        except Exception as e:
            logger.error(f"Error processing {source}: {e}")
            success = False

    if not success:
        if x in data:
            data[x]["processed"] = False
        return None

    if x in data:
        data[x]["processed"] = True
    return x


def extract_buffer(buffer: BytesIO, x: str, k: str) -> None:
    """Decompresses and extracts the buffer to disk, keeping the original archive."""
    try:
        out_dir = Path(x)
        out_dir.mkdir(parents=True, exist_ok=True)

        raw_bytes = buffer.getvalue()
        archive_file = out_dir / k
        archive_file.parent.mkdir(parents=True, exist_ok=True)
        if not (archive_file.exists() and archive_file.stat().st_size == len(raw_bytes)):
            with open(archive_file, "wb") as f_arch:
                f_arch.write(raw_bytes)

        buffer.seek(0)
        decompressor = ZstdDecompressor()
        working_buffer = BytesIO()
        try:
            decompressor.copy_stream(buffer, working_buffer)
            working_buffer.seek(0)
            is_zstd = True
        except Exception:
            buffer.seek(0)
            working_buffer = buffer
            is_zstd = False

        try:
            with tar_open(fileobj=working_buffer, mode="r") as tar:
                for member in tar.getmembers():
                    f_extracted = tar.extractfile(member)
                    if f_extracted:
                        content = f_extracted.read()
                        member_out = out_dir / member.name
                        member_out.parent.mkdir(parents=True, exist_ok=True)
                        with open(member_out, "wb") as f_out:
                            f_out.write(content)
                        if Path(member.name).name == "orca.gbw.zstd0":
                            gbw_content = extract_gbw_bytes(content)
                            with open(out_dir / "orca.gbw", "wb") as f_gbw:
                                f_gbw.write(gbw_content)
        except tarfile.TarError:
            working_buffer.seek(0)
            out_name = Path(k).name
            if is_zstd:
                for suffix in [".zst", ".zstd0", ".zstd"]:
                    if out_name.endswith(suffix):
                        out_name = out_name[: -len(suffix)]
                        break
            content = working_buffer.read()
            if Path(k).name.startswith("orca.gbw") and (
                Path(k).name.endswith((".zstd0", ".zst", ".zstd")) or is_zstd
            ):
                content = extract_gbw_bytes(content)
                out_name = "orca.gbw"
            out_path = out_dir / out_name
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if out_path != archive_file or not archive_file.exists():
                with open(out_path, "wb") as f_out:
                    f_out.write(content)
    except Exception as e:
        logger.error(f"Extraction error for {x}: {e}")
        raise


def save_restart_file(restart_file: Union[str, Path], data: Dict[str, Any]) -> None:
    """Atomically saves restart state to JSON."""
    rf = Path(restart_file)
    tmp_file = rf.with_name(f"{rf.name}.tmp")
    try:
        with open(tmp_file, "w", encoding="utf-8") as f:
            json_dump(data, f, indent=4)
        tmp_file.replace(rf)
    except Exception as e:
        logger.error(f"Failed to write restart file {rf}: {e}")
        tmp_file.unlink(missing_ok=True)


def manager_loop(keys: List[str], data: Dict[str, Any], restart_file: Union[str, Path], comm: Any, size: int) -> None:
    """Rank 0 Dispatcher for downloads (Synchronous)."""
    start_time = time.time()
    logger.info(f"Download Manager starting with {size - 1} workers.")

    pending_keys = list(keys)
    active_workers = size - 1
    processed_count = 0
    pbar = tqdm(total=len(pending_keys), desc="Download Progress")

    while active_workers > 0:
        status = MPI.Status()
        msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()

        if msg == "READY":
            if pending_keys:
                key = pending_keys.pop(0)
                comm.send(key, dest=source)
            else:
                comm.send(None, dest=source)
                active_workers -= 1
        elif isinstance(msg, tuple):  # (key, success)
            key, success = msg
            if key in data:
                data[key]["processed"] = bool(success)
            processed_count += 1
            pbar.update(1)

            if processed_count % 100 == 0:
                save_restart_file(restart_file, data)
        elif isinstance(msg, str):  # prefix (legacy string success)
            if msg in data:
                data[msg]["processed"] = True
            processed_count += 1
            pbar.update(1)

            if processed_count % 100 == 0:
                save_restart_file(restart_file, data)

    pbar.close()
    save_restart_file(restart_file, data)
    logger.info(f"Download complete in {time.time() - start_time:.2f} seconds.")


def worker_loop(data: Dict[str, Any], args: argparse.Namespace, comm: Any) -> None:
    """Rank > 0 Downloader (Synchronous)."""
    s3_client = None
    if not args.local_dir:
        with open(args.login_file, "r") as f:
            creds = json_load(f)
        s3_client = boto3.client(
            "s3",
            region_name="us-east-1",
            endpoint_url="https://s3.echo.stfc.ac.uk",
            aws_access_key_id=creds["access_key"],
            aws_secret_access_key=creds["secret_key"],
            config=Config(retries={"max_attempts": 5}),
        )

    try:
        while True:
            comm.send("READY", dest=0)
            key = comm.recv(source=0, tag=MPI.ANY_TAG)
            if key is None:
                break

            res = process_prefix(key, data, args, s3_client)
            comm.send((key, bool(res)), dest=0)
    finally:
        pass


def download_serial(keys: List[str], data: Dict[str, Any], args: argparse.Namespace) -> None:
    """Serial download (Synchronous)."""
    start_time = time.time()
    s3_client = None
    if not args.local_dir:
        with open(args.login_file, "r") as f:
            creds = json_load(f)
        s3_client = boto3.client(
            "s3",
            region_name="us-east-1",
            endpoint_url="https://s3.echo.stfc.ac.uk",
            aws_access_key_id=creds["access_key"],
            aws_secret_access_key=creds["secret_key"],
        )

    try:
        for x in tqdm(keys, desc="Serial Download"):
            res = process_prefix(x, data, args, s3_client)
            if res:
                data[res]["processed"] = True
            else:
                data[x]["processed"] = False

        logger.info(f"Download complete in {time.time() - start_time:.2f} seconds.")
    finally:
        pass



def main() -> None:
    args = parse_args()

    if getattr(args, "tmp_dir", None):
        custom_tmp = get_shm_temp_dir(args.tmp_dir)
        if custom_tmp:
            tempfile.tempdir = custom_tmp

    if args.mpi:
        comm = MPI.COMM_WORLD
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

    if rank == 0:
        logfile = (
            args.log_file
            if args.log_file
            else Path(args.data_source.stem + "_download.log")
        )
        setup_logging(
            level=log_level_map.get(args.log_level.upper(), logging.INFO),
            log_file_path=logfile,
        )

    with open(args.data_source, "r", encoding="utf-8") as f:
        full_data = json_load(f)

    all_keys = list(full_data.keys())
    total_keys = len(all_keys)

    is_restart_file = args.data_source.name.endswith("_restart.json")
    if is_restart_file:
        args.restart = True

    stem = args.data_source.stem
    stem_no_restart = stem[:-8] if stem.endswith("_restart") else stem
    m_chunk = re.search(r"^(.*?)(?:_(\d+)_(\d+))$", stem_no_restart)
    is_chunk = False
    if m_chunk:
        stem_base = m_chunk.group(1)
        restart_file = args.data_source
        assigned_keys = all_keys
        is_chunk = True
    elif (
        getattr(args, "start_index", None) is not None
        and (args.start_index > 0 or (0 < getattr(args, "sample_size", -1) < total_keys))
    ):
        stem_base = stem_no_restart
        start_idx = args.start_index
        sample_size = getattr(args, "sample_size", -1)
        if sample_size is None or sample_size < 0:
            end_idx = total_keys
        else:
            end_idx = min(start_idx + sample_size, total_keys)
        is_chunk = True
        chunk_tag = f"_{start_idx}_{end_idx}"
        restart_file = args.data_source.with_name(f"{stem_base}{chunk_tag}_restart.json")
        if args.restart and restart_file.exists():
            try:
                with open(restart_file, "r", encoding="utf-8") as f_rst:
                    saved_chunk_data = json_load(f_rst)
                for k, v in saved_chunk_data.items():
                    if k in full_data and isinstance(v, dict):
                        full_data[k].update(v)
            except Exception as e_rst:
                logger.warning(f"Could not read restart file {restart_file}: {e_rst}")
        assigned_keys = all_keys[start_idx : end_idx]
    else:
        stem_base = stem_no_restart
        is_chunk = False
        if is_restart_file:
            restart_file = args.data_source
        else:
            restart_file = args.data_source.with_name(f"{stem_base}_restart.json")
        if args.restart and not is_restart_file and restart_file.exists():
            try:
                with open(restart_file, "r", encoding="utf-8") as f_rst:
                    saved_rst = json_load(f_rst)
                for k, v in saved_rst.items():
                    if k in full_data and isinstance(v, dict):
                        full_data[k].update(v)
            except Exception as e_rst:
                logger.warning(f"Could not read restart file {restart_file}: {e_rst}")
        assigned_keys = all_keys

    data = {k: full_data[k] for k in assigned_keys}

    if args.restart:
        keys = [x for x in data if not data[x].get("processed", False)]
    else:
        keys = list(data.keys())

    if not is_chunk and getattr(args, "sample_size", -1) is not None and args.sample_size > 0:
        keys = keys[: args.sample_size]

    if size > 1:
        if rank == 0:
            manager_loop(keys, data, restart_file, comm, size)
        else:
            worker_loop(data, args, comm)
    else:
        download_serial(keys, data, args)
        save_restart_file(restart_file, data)


if __name__ == "__main__":
    main()
