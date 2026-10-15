import logging
import sys
from pathlib import Path
from typing import Optional, Union, Any, IO

import orjson

def json_load(fp: IO) -> Any:
    """Wrapper around orjson.loads to act as a drop-in replacement for json.load."""
    return orjson.loads(fp.read())

def json_dump(obj: Any, fp: IO, indent: Optional[int] = None) -> None:
    """Wrapper around orjson.dumps to act as a drop-in replacement for json.dump."""
    option = orjson.OPT_INDENT_2 if indent else 0
    data = orjson.dumps(obj, option=option)
    if "b" in getattr(fp, "mode", "w"):
        fp.write(data)
    else:
        fp.write(data.decode("utf-8"))

def setup_logging(level: int = logging.INFO, log_file_path: Optional[Union[str, Path]] = None) -> None:
    """Configures the root logger with a console handler and an optional file handler."""
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    # Console handler (to stdout for tests to capture)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    handlers = [console_handler]
    status_msg = "Console only"

    if log_file_path:
        p = Path(log_file_path)
        file_handler = logging.FileHandler(p, mode="a")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
        status_msg = f"Logfile: {p.resolve()}"

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)
    for h in handlers:
        root_logger.addHandler(h)

    # Use a logger for the utilities module to log initialization
    util_logger = logging.getLogger(__name__)
    util_logger.info(
        f"Logger initialized at level: {logging.getLevelName(level)} ({status_msg})"
    )


def extract_gbw_bytes(raw_bytes: bytes) -> bytes:
    """
    Extract orca.gbw bytes from raw bytes that may be zstd-compressed (e.g. orca.gbw.zstd0)
    or a zstd-compressed tar archive.
    """
    from io import BytesIO
    import tarfile
    from zstandard import ZstdDecompressor

    decompressor = ZstdDecompressor()
    decompressed_buffer = BytesIO()
    try:
        decompressor.copy_stream(BytesIO(raw_bytes), decompressed_buffer)
        decompressed_buffer.seek(0)
    except Exception:
        # If not zstd-compressed or decompression fails, return raw_bytes
        return raw_bytes

    # If the decompressed buffer is a tar archive, extract orca.gbw from it
    try:
        with tarfile.open(fileobj=decompressed_buffer, mode="r") as tar:
            for member in tar.getmembers():
                if Path(member.name).name == "orca.gbw":
                    f = tar.extractfile(member)
                    if f:
                        return f.read()
            for member in tar.getmembers():
                f = tar.extractfile(member)
                if f:
                    return f.read()
    except tarfile.TarError:
        pass

    decompressed_buffer.seek(0)
    return decompressed_buffer.read()


def extract_gbw_from_path(gbw_path: Path) -> Optional[bytes]:
    """
    Read orca.gbw from path, checking for orca.gbw or orca.gbw.zstd0 in the parent directory.
    If orca.gbw.zstd0 exists, extracts orca.gbw, writes it to disk, and returns its bytes.
    """
    if gbw_path.is_file():
        return gbw_path.read_bytes()

    zstd_candidates = [
        gbw_path.with_name("orca.gbw.zstd0"),
        gbw_path.with_name("orca.gbw.zst"),
        gbw_path.with_name("orca.gbw.zstd"),
    ]
    for cand in zstd_candidates:
        if cand.is_file():
            gbw_bytes = extract_gbw_bytes(cand.read_bytes())
            try:
                gbw_path.write_bytes(gbw_bytes)
            except Exception:
                pass
            return gbw_bytes
    return None
