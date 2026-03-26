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
