import argparse
import logging
import asyncio
from pathlib import Path
from json import load as json_load, dump as json_dump
from io import BytesIO
import tarfile
from tarfile import open as tar_open
from zstandard import ZstdDecompressor
from botocore.config import Config
from aiobotocore.session import get_session
from tqdm.asyncio import tqdm as async_tqdm
import time

from .process_omol25 import setup_logging, get_ranges

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Download and extract molecular data asynchronously.")
    parser.add_argument(
        "--login-file",
        type=Path,
        required=False,
        default=None,
        help="Path to credentials JSON.",
    )
    parser.add_argument("--bucket", type=str, default="omol-25-small", help="S3 bucket.")
    parser.add_argument("--data-source", type=Path, required=True, help="JSON file with keys.")
    parser.add_argument("--sample-size", type=int, default=-1, help="Number of samples to run.")
    parser.add_argument("--start-index", type=int, default=0, help="Start index.")
    parser.add_argument("--restart", action="store_true", help="Skip configurations marked as processed.")
    parser.add_argument("--local-dir", type=Path, default=None, help="Optional local directory to read data from instead of S3.")
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
    return parser.parse_args()


async def process_prefix(x, data, args, s3_client, semaphore):
    """Processes a single prefix asynchronously."""
    async with semaphore:
        raw_key = data[x]['key']
        key_list = [raw_key] if isinstance(raw_key, str) else raw_key
        
        success = True
        file_start_time = time.time()
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
                    content = await asyncio.to_thread(local_path.read_bytes)
                    buffer.write(content)
                else:
                    response = await s3_client.get_object(Bucket=args.bucket, Key=source)
                    async with response['Body'] as stream:
                        buffer.write(await stream.read())
                
                buffer.seek(0)

                # Extraction logic (Offload to thread)
                await asyncio.to_thread(extract_buffer, buffer, x, k)

            except Exception as e:
                logger.error(f"Error processing {source}: {e}")
                success = False
        
        if success:
            logger.info(f"Processed {x} in {time.time() - file_start_time:.2f} seconds.")
            return x
        return None

def extract_buffer(buffer: BytesIO, x: str, k: str):
    """Decompresses and extracts the buffer to disk."""
    try:
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

        out_dir = Path(x)
        out_dir.mkdir(parents=True, exist_ok=True)

        try:
            with tar_open(fileobj=working_buffer, mode="r") as tar:
                for member in tar.getmembers():
                    f_extracted = tar.extractfile(member)
                    if f_extracted:
                        content = f_extracted.read()
                        with open(out_dir / member.name, "wb") as f_out:
                            f_out.write(content)
        except tarfile.TarError:
            working_buffer.seek(0)
            out_name = Path(k).name
            if is_zstd:
                for suffix in ['.zst', '.zstd0', '.zstd']:
                    if out_name.endswith(suffix):
                        out_name = out_name[:-len(suffix)]
                        break
            with open(out_dir / out_name, "wb") as f_out:
                f_out.write(working_buffer.read())
    except Exception as e:
        logger.error(f"Extraction error for {x}: {e}")
        raise

async def download_async(args, rank, size, comm):
    """Main async download loop."""
    start_time = time.time()
    
    if not args.local_dir and not args.login_file:
        raise ValueError("--login-file is required when --local-dir is not specified.")

    with open(args.data_source, "r", encoding="utf-8") as f:
        data = json_load(f)

    if args.data_source.name.endswith("_restart.json"):
        restart_file = args.data_source
    else:
        restart_file = args.data_source.with_name(args.data_source.stem + "_restart.json")

    if args.restart:
        keys = [x for x in data if not data[x].get("processed", False)]
    else:
        keys = list(data.keys())

    if args.sample_size > 0:
        keys = keys[args.start_index:args.start_index + args.sample_size]
    else:
        keys = keys[args.start_index:]

    batches = get_ranges(keys, size)
    my_batch = batches[rank]

    s3_client = None
    if not args.local_dir:
        with open(args.login_file, "r") as f:
            creds = json_load(f)
        session = get_session()
        s3_client_cm = session.create_client(
            's3',
            region_name='us-east-1',
            endpoint_url="https://s3.echo.stfc.ac.uk",
            aws_access_key_id=creds["access_key"],
            aws_secret_access_key=creds["secret_key"],
            config=Config(retries={'max_attempts': 5})
        )
        s3_client = await s3_client_cm.__aenter__()

    try:
        semaphore = asyncio.Semaphore(10)
        tasks = [process_prefix(x, data, args, s3_client, semaphore) for x in my_batch]
        
        processed_successfully = []
        for coro in async_tqdm.as_completed(tasks, desc=f"rank {rank}",
                                               disable=(rank != 0 and len(my_batch) > 5)):
            res = await coro
            if res:
                processed_successfully.append(res)
        
        if comm:
            all_processed = await asyncio.to_thread(comm.gather, processed_successfully, 0)
        else:
            all_processed = [processed_successfully]
        
        if rank == 0:
            flat_processed = [p for sublist in all_processed if sublist for p in sublist]
            for p in flat_processed:
                data[p]['processed'] = True
            
            with open(restart_file, "w") as f:
                json_dump(data, f, indent=4)
            logger.info(f"Restart data updated in {restart_file.resolve()}")
            logger.info(f"Download complete in {time.time() - start_time:.2f} seconds.")
            
    finally:
        if s3_client:
            await s3_client_cm.__aexit__(None, None, None)

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

    if rank == 0:
        logfile = args.log_file if args.log_file else Path(args.data_source.stem + "_download.log")
        setup_logging(
            level=log_level_map.get(args.log_level.upper(), logging.INFO),
            log_file_path=logfile
        )

    asyncio.run(download_async(args, rank, size, comm))


if __name__ == "__main__":
    main()
