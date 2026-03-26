import logging
from pathlib import Path
from typing import Optional, Union

import boto3
from botocore.config import Config

from .utils import json_load

logger = logging.getLogger(__name__)

class S3DataProcessor:
    """
    Base class for S3 and local data access logic.
    Focuses on credentials and client initialization.
    """

    def __init__(self, login_file: Optional[Union[str, Path]], bucket: str, local_dir: Optional[Union[str, Path]] = None) -> None:
        self.login_file = login_file
        self.bucket = bucket
        self.local_dir = Path(local_dir) if local_dir else None
        self.s3_endpoint_url = "https://s3.echo.stfc.ac.uk"
        self.creds = None

        if not self.local_dir and self.login_file:
            with open(self.login_file, "r") as f:
                self.creds = json_load(f)

    def get_s3_client(self):
        """Initializes and returns a boto3 S3 client if not in local mode."""
        if self.local_dir:
            return None
        
        if not self.creds:
            raise ValueError("Credentials are required for S3 access when local_dir is not provided.")

        return boto3.client(
            "s3",
            region_name="us-east-1",
            endpoint_url=self.s3_endpoint_url,
            aws_access_key_id=self.creds["access_key"],
            aws_secret_access_key=self.creds["secret_key"],
            config=Config(retries={"max_attempts": 5}),
        )
