import argparse
import logging
from pathlib import Path
from json import load as json_load
from io import BytesIO
import tarfile
from tarfile import open as tar_open
from zstandard import ZstdDecompressor
from botocore.config import Config
from boto3 import client as s3_client
from tqdm import tqdm
import time

from .process_omol25 import s3_transfer_options, setup_logging, get_ranges

logger = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Download and extract molecular data in memory.")
    parser.add_argument("--login-file", type=Path, required=True, help="Path to credentials JSON.")
    parser.add_argument("--bucket", type=str, default="omol-25-small", help="S3 bucket.")
    parser.add_argument("--data-source", type=Path, required=True, help="JSON file with keys.")
    parser.add_argument("--sample-size", type=int, default=-1, help="Number of samples to run.")
    parser.add_argument("--start-index", type=int, default=0, help="Start index.")
    parser.add_argument("--restart", action="store_true", help="Skip configurations marked as processed.")
    parser.add_argument("--local-dir", type=Path, default=None, help="Optional local directory to read data from instead of S3.")
    parser.add_argument("--mpi", action="store_true", help="Run with MPI.")
    return parser.parse_args()

def initialize_s3_client(login_file: Path):
    with open(login_file, "r") as f:
        credentials = json_load(f)
    config = Config(
        s3={"addressing_style": "path"},
        retries={'max_attempts': 5, 'mode': 'standard'},
        connect_timeout=5,
        read_timeout=30,
        max_pool_connections=20
    )
    return s3_client(
        "s3",
        config=config,
        endpoint_url="https://s3.echo.stfc.ac.uk",
        aws_access_key_id=credentials["access_key"],
        aws_secret_access_key=credentials["secret_key"],
    )

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

    if rank == 0:
        setup_logging(logging.INFO, Path(args.data_source.stem + "_download.log"))

    with open(args.data_source, "r", encoding="utf-8") as f:
        data = json_load(f)

    if args.data_source.name.endswith("_restart.json"):
        restart_file = args.data_source
    else:
        restart_file = args.data_source.with_name(args.data_source.stem + "_restart.json")

    # If restart is requested, skip already processed
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

    s3 = initialize_s3_client(args.login_file)
    logger.info(f"Rank {rank} processing {len(my_batch)} files.")

    start_time = time.time()
    processed_successfully = []
    
    pbar = tqdm(total=len(my_batch), desc=f"rank {rank}", disable=(rank != 0 and len(my_batch) > 100))
    for x in my_batch:
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
                    buffer.write(local_path.read_bytes())
                else:
                    s3.download_fileobj(
                        Bucket=args.bucket,
                        Key=source,
                        Fileobj=buffer,
                        Config=s3_transfer_options,
                    )
                buffer.seek(0)

                try:
                    decompressor = ZstdDecompressor()
                    working_buffer = BytesIO()
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
                    # Fallback for plain files or single ZSTD compressed files
                    working_buffer.seek(0)
                    out_name = Path(k).name
                    if is_zstd:
                        if out_name.endswith('.zst'):
                            out_name = out_name[:-4]
                        elif out_name.endswith('.zstd0'):
                            out_name = out_name[:-6]
                        elif out_name.endswith('.zstd'):
                            out_name = out_name[:-5]
                    with open(out_dir / out_name, "wb") as f_out:
                        f_out.write(working_buffer.read())
            except Exception as e:
                logger.error(f"Error processing {source}: {e}")
                success = False
            
        if success:
            processed_successfully.append(x)
            logger.info(f"Processed {x} in {time.time() - file_start_time:.2f} seconds.")
        pbar.update(1)
        
    pbar.close()

    if comm:
        all_processed = comm.gather(processed_successfully, root=0)
    else:
        all_processed = [processed_successfully]
    
    if rank == 0:
        flat_processed = [p for sublist in all_processed if sublist for p in sublist]
        for p in flat_processed:
            data[p]['processed'] = True
        
        from json import dump as json_dump
        with open(restart_file, "w") as f:
            json_dump(data, f, indent=4)
        logger.info(f"Restart data updated in {restart_file.resolve()}")
        logger.info(f"Download complete in {time.time() - start_time:.2f} seconds.")


if __name__ == "__main__":
    main()
