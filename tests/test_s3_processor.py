import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from lavello_mlips.s3_processor import S3DataProcessor

def test_s3_processor_local_mode():
    """Test that S3DataProcessor correctly handles local mode."""
    processor = S3DataProcessor(login_file=None, bucket="test-bucket", local_dir="/tmp/local_data")
    assert processor.local_dir == Path("/tmp/local_data")
    assert processor.get_s3_client() is None

def test_s3_processor_s3_mode(tmp_path):
    """Test that S3DataProcessor correctly initializes S3 client with credentials."""
    login_file = tmp_path / "login.json"
    creds = {"access_key": "test_access", "secret_key": "test_secret"}
    login_file.write_text(json.dumps(creds))
    
    with patch("lavello_mlips.s3_processor.boto3.client") as mock_client:
        mock_client.return_value = MagicMock()
        
        processor = S3DataProcessor(login_file=str(login_file), bucket="test-bucket")
        client = processor.get_s3_client()
        
        assert client is not None
        mock_client.assert_called_once()
        args, kwargs = mock_client.call_args
        assert kwargs["aws_access_key_id"] == "test_access"
        assert kwargs["aws_secret_access_key"] == "test_secret"
        assert kwargs["endpoint_url"] == "https://s3.echo.stfc.ac.uk"

def test_s3_processor_missing_creds_error():
    """Test that S3DataProcessor raises ValueError when credentials are missing in S3 mode."""
    processor = S3DataProcessor(login_file=None, bucket="test-bucket", local_dir=None)
    with pytest.raises(ValueError, match="Credentials are required"):
        processor.get_s3_client()

def test_s3_processor_invalid_login_file():
    """Test that S3DataProcessor raises FileNotFoundError with invalid login file path."""
    with pytest.raises(FileNotFoundError):
        S3DataProcessor(login_file="non_existent.json", bucket="test-bucket")
