import json
import os
import subprocess
import sys
import tarfile
import io
from pathlib import Path
from unittest.mock import patch
from zstandard import ZstdCompressor

import pytest
from lavello_mlips.download_omol25 import main as download_main

def create_mock_tar_zst(path: Path, files: dict):
    """Creates a mock .tar.zst file with given file contents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content.encode("utf-8") if isinstance(content, str) else content))
    
    compressor = ZstdCompressor()
    compressed = compressor.compress(tar_buffer.getvalue())
    path.write_bytes(compressed)

def test_download_serial_coverage(tmp_path, monkeypatch):
    """Test serial download calling main() directly for coverage."""
    local_data_dir = tmp_path / "mock_s3_cov"
    data_source_json = tmp_path / "data_source_cov.json"
    dest_dir = tmp_path / "downloads_cov"
    dest_dir.mkdir()
    
    # Setup mock data
    prefix = "mol_1_cov/"
    key = "data.tar.zst"
    mock_files = {
        "orca.out": "MOCK ORCA OUTPUT COV",
        "orca.inp": "MOCK INPUT COV",
        "orca.property.txt": "MOCK PROPERTY COV"
    }
    create_mock_tar_zst(local_data_dir / prefix / key, mock_files)
    
    data_source = {prefix: {"key": key, "processed": False}}
    data_source_json.write_text(json.dumps(data_source))
    
    # Call main() directly
    test_args = [
        "download_omol25",
        "--data-source", str(data_source_json),
        "--local-dir", str(local_data_dir)
    ]
    
    monkeypatch.chdir(dest_dir)
    with patch.object(sys, "argv", test_args):
        download_main()
        
    # Verify extraction
    for name in mock_files:
        extracted_file = dest_dir / prefix / name
        assert extracted_file.exists()
        assert extracted_file.read_text() == mock_files[name]
            
    # Verify restart file
    restart_file = data_source_json.with_name(data_source_json.stem + "_restart.json")
    assert restart_file.exists()
    with open(restart_file, "r") as f:
        restart_data = json.load(f)
    assert restart_data[prefix]["processed"] is True

def test_download_single_zst_coverage(tmp_path, monkeypatch):
    """Test downloading a single .zst file calling main() directly."""
    local_data_dir = tmp_path / "mock_s3_zst_cov"
    data_source_json = tmp_path / "data_source_zst_cov.json"
    dest_dir = tmp_path / "downloads_zst_cov"
    dest_dir.mkdir()
    
    prefix = "mol_z_cov/"
    key = "output.zst"
    content = b"RAW ZSTD CONTENT COV"
    
    path = local_data_dir / prefix / key
    path.parent.mkdir(parents=True, exist_ok=True)
    compressor = ZstdCompressor()
    path.write_bytes(compressor.compress(content))
    
    data_source = {prefix: {"key": key, "processed": False}}
    data_source_json.write_text(json.dumps(data_source))
    
    test_args = [
        "download_omol25",
        "--data-source", str(data_source_json),
        "--local-dir", str(local_data_dir)
    ]
    
    monkeypatch.chdir(dest_dir)
    with patch.object(sys, "argv", test_args):
        download_main()
        
    # Verify extraction
    extracted_file = dest_dir / prefix / "output"
    assert extracted_file.exists()
    assert extracted_file.read_bytes() == content

def test_download_mpi_functional(tmp_path, monkeypatch):
    """Functional MPI test (subprocess) to ensure orchestration works."""
    local_data_dir = tmp_path / "mock_s3_mpi"
    data_source_json = tmp_path / "data_source_mpi.json"
    dest_dir = tmp_path / "downloads_mpi"
    dest_dir.mkdir()
    
    prefixes = ["mol_a/", "mol_b/"]
    key = "orca.tar.zst"
    mock_files = {"orca.out": "LOG"}
    
    data_source = {}
    for p in prefixes:
        create_mock_tar_zst(local_data_dir / p / key, mock_files)
        data_source[p] = {"key": key, "processed": False}
    
    data_source_json.write_text(json.dumps(data_source))
    
    monkeypatch.chdir(dest_dir)
    cmd = [
        "mpirun", "--oversubscribe", "-n", "3",
        sys.executable,
        "-m", "lavello_mlips.download_omol25",
        "--data-source", str(data_source_json),
        "--local-dir", str(local_data_dir),
        "--mpi"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0
    
    for p in prefixes:
        assert (dest_dir / p / "orca.out").exists()

def test_download_sample_and_restart(tmp_path, monkeypatch):
    """Test sample-size and restart logic in download_omol25.py."""
    local_data_dir = tmp_path / "mock_s3_restart"
    data_source_json = tmp_path / "data_source_restart.json"
    dest_dir = tmp_path / "downloads_restart"
    dest_dir.mkdir()
    
    # Setup 3 molecules
    prefixes = [f"mol_{i}/" for i in range(3)]
    for p in prefixes:
        create_mock_tar_zst(local_data_dir / p / "data.tar.zst", {"out": "DATA"})
    
    # Prefix 0 is already processed
    data_source = {
        prefixes[0]: {"key": "data.tar.zst", "processed": True},
        prefixes[1]: {"key": "data.tar.zst", "processed": False},
        prefixes[2]: {"key": "data.tar.zst", "processed": False}
    }
    data_source_json.write_text(json.dumps(data_source))
    
    # Run with restart and sample-size=1 (should only do mol_1)
    test_args = [
        "download_omol25",
        "--data-source", str(data_source_json),
        "--local-dir", str(local_data_dir),
        "--restart",
        "--sample-size", "1"
    ]
    
    monkeypatch.chdir(dest_dir)
    with patch.object(sys, "argv", test_args):
        download_main()
        
    assert (dest_dir / prefixes[1] / "out").exists()
    assert not (dest_dir / prefixes[2] / "out").exists()
    assert not (dest_dir / prefixes[0] / "out").exists() # Prefix 0 was skipped

def test_extract_buffer_robustness(tmp_path):
    """Test that extract_buffer handles non-zstd non-tar data by just writing it."""
    from lavello_mlips.download_omol25 import extract_buffer
    content = b"just some text"
    buffer = io.BytesIO(content)
    prefix = tmp_path / "robust_prefix"
    
    extract_buffer(buffer, str(prefix), "data.txt")
    
    assert (prefix / "data.txt").exists()
    assert (prefix / "data.txt").read_bytes() == content

def test_process_prefix_file_not_found(tmp_path, monkeypatch):
    """Test process_prefix when local file is missing."""
    from lavello_mlips.download_omol25 import process_prefix
    
    class MockArgs:
        local_dir = tmp_path / "empty_dir"
        bucket = "mock"
    
    MockArgs.local_dir.mkdir()
    
    data = {"mol_1/": {"key": "missing.tar.zst"}}
    
    # Use monkeypatch for safety
    monkeypatch.chdir(tmp_path)
    # Should log warning and return None
    res = process_prefix("mol_1/", data, MockArgs(), None)
    assert res is None

def test_process_prefix_s3(tmp_path, monkeypatch):
    """Test process_prefix in S3 mode with mock client."""
    from lavello_mlips.download_omol25 import process_prefix
    from unittest.mock import MagicMock
    
    class MockArgs:
        local_dir = None
        bucket = "test-bucket"
    
    data = {"mol_s3/": {"key": "data.tar.zst"}}
    s3_client = MagicMock()
    s3_client.get_object.return_value = {
        "Body": MagicMock(read=lambda: b"mock content")
    }
    
    monkeypatch.chdir(tmp_path)
    res = process_prefix("mol_s3/", data, MockArgs(), s3_client)
    assert res == "mol_s3/"
    assert (tmp_path / "mol_s3/data.tar.zst").exists()

def test_extract_buffer_zstd_not_tar(tmp_path):
    """Test extract_buffer with valid zstd but invalid tar."""
    from lavello_mlips.download_omol25 import extract_buffer
    from zstandard import ZstdCompressor
    
    content = b"not a tar"
    c = ZstdCompressor()
    compressed = c.compress(content)
    
    buffer = io.BytesIO(compressed)
    prefix = tmp_path / "zstd_not_tar"
    
    # It should decompress, fail tar, and write 'data' (removing .zst)
    extract_buffer(buffer, str(prefix), "data.zst")
    
    assert (prefix / "data").exists()
    assert (prefix / "data").read_bytes() == content

def test_process_prefix_error(tmp_path):
    """Test process_prefix error handling."""
    from lavello_mlips.download_omol25 import process_prefix
    from unittest.mock import MagicMock
    
    class MockArgs:
        local_dir = None
        bucket = "test-bucket"
    
    data = {"mol_err/": {"key": "data.tar.zst"}}
    s3_client = MagicMock()
    s3_client.get_object.side_effect = Exception("S3 Error")
    
    res = process_prefix("mol_err/", data, MockArgs(), s3_client)
    assert res is None

def test_main_s3_error(tmp_path, monkeypatch):
    """Test main with S3 error to hit more lines."""
    from lavello_mlips.download_omol25 import main as download_main
    from unittest.mock import MagicMock, patch
    
    data_source_json = tmp_path / "data_source_s3_err.json"
    login_file = tmp_path / "login.json"
    login_file.write_text(json.dumps({"access_key": "x", "secret_key": "y"}))
    
    data_source = {"mol_1/": {"key": "data.tar.zst", "processed": False}}
    data_source_json.write_text(json.dumps(data_source))
    
    test_args = [
        "download_omol25",
        "--data-source", str(data_source_json),
        "--login-file", str(login_file),
        "--bucket", "test-bucket"
    ]
    
    monkeypatch.chdir(tmp_path)
    with patch("lavello_mlips.download_omol25.boto3.client") as mock_boto:
        mock_cli = MagicMock()
        mock_cli.get_object.side_effect = Exception("S3 error")
        mock_boto.return_value = mock_cli
        
        with patch.object(sys, "argv", test_args):
            download_main()
            
    # Verify prefix not marked processed
    restart_file = data_source_json.with_name(data_source_json.stem + "_restart.json")
    with open(restart_file, "r") as f:
        data = json.load(f)
    assert data["mol_1/"]["processed"] is False

def test_manager_loop_mocked(tmp_path):
    """Test manager_loop by mocking MPI communication."""
    from lavello_mlips.download_omol25 import manager_loop
    from unittest.mock import MagicMock, patch
    
    comm = MagicMock()
    status = MagicMock()
    status.Get_source.return_value = 1
    
    # READY (worker wants task) -> sends mol_1/
    # mol_1/ (worker finished) -> manager marks processed
    # READY (worker wants task) -> sends None (no more tasks)
    comm.recv.side_effect = ["READY", "mol_1/", "READY"]
    
    keys = ["mol_1/"]
    data = {"mol_1/": {"processed": False}}
    restart_file = tmp_path / "restart_mpi_mock.json"
    
    with patch("lavello_mlips.download_omol25.MPI.Status", return_value=status), \
         patch("lavello_mlips.download_omol25.tqdm", return_value=MagicMock()):
        manager_loop(keys, data, restart_file, comm, size=2)
        
    assert data["mol_1/"]["processed"] is True
    assert restart_file.exists()

def test_worker_loop_mocked():
    """Test worker_loop by mocking MPI communication."""
    from lavello_mlips.download_omol25 import worker_loop
    from unittest.mock import MagicMock, patch
    
    comm = MagicMock()
    # mol_1/ (task assigned)
    # None (exit signal)
    comm.recv.side_effect = ["mol_1/", None]
    
    class MockArgs:
        local_dir = "/tmp"
        login_file = None
        
    with patch("lavello_mlips.download_omol25.process_prefix", return_value="mol_1/"):
        worker_loop({}, MockArgs(), comm)
        
    # Worker sends READY then task result, then READY then exits
    # total 3 sends: READY, mol_1/, READY
    assert comm.send.call_count == 3
