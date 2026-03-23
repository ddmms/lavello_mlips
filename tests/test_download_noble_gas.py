import json
import os
import shutil
import sys
import tarfile
import io
from pathlib import Path
from unittest.mock import patch
from zstandard import ZstdCompressor

import pytest
from lavello_mlips.download_omol25 import main as download_main

def package_real_assets(prefix: str, keys: list, src_root: Path, target_dir: Path):
    """
    Finds real files in src_root / prefix and packages them into target_dir / prefix / key.
    """
    prefix_path = src_root / prefix
    if not prefix_path.exists():
        return False

    (target_dir / prefix).mkdir(parents=True, exist_ok=True)
    
    for key in keys:
        target_path = target_dir / prefix / key
        if key == "orca.tar.zst":
            # Package orca.out, orca.inp, orca.property.txt
            tar_buffer = io.BytesIO()
            with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
                for name in ["orca.out", "orca.inp", "orca.property.txt"]:
                    fpath = prefix_path / name
                    if fpath.exists():
                        tar.add(fpath, arcname=name)
            
            compressor = ZstdCompressor()
            compressed = compressor.compress(tar_buffer.getvalue())
            target_path.write_bytes(compressed)
            
        elif key.endswith(".zstd0") or key.endswith(".zst"):
            # Compress the base file
            base_name = key.replace(".zstd0", "").replace(".zst", "")
            fpath = prefix_path / base_name
            if fpath.exists():
                compressor = ZstdCompressor()
                target_path.write_bytes(compressor.compress(fpath.read_bytes()))
        else:
            # Copy as is (e.g. .npz)
            fpath = prefix_path / key
            if fpath.exists():
                shutil.copy2(fpath, target_path)
    return True

def test_download_noble_gas_real(tmp_path, monkeypatch):
    """Test download_omol25.py using real noble gas data assets."""
    # Paths
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    prefix_json = data_dir / "noble_gas_compounds_prefix_full.json"
    real_assets_root = data_dir # Prefixes in JSON start with noble_gas_compounds/

    if not prefix_json.exists():
        pytest.skip("noble_gas_compounds_prefix_full.json not found")

    # Load JSON and take first 3 prefixes
    with open(prefix_json, "r") as f:
        full_data = json.load(f)
    
    test_prefixes = list(full_data.keys())[:3]
    test_data = {p: full_data[p] for p in test_prefixes}
    
    # Setup mock S3 (local-dir)
    mock_s3_dir = tmp_path / "mock_s3_real"
    mock_s3_dir.mkdir()
    
    found_any = False
    for p in test_prefixes:
        if package_real_assets(p, test_data[p]["key"], real_assets_root, mock_s3_dir):
            found_any = True
            
    if not found_any:
        pytest.skip("No real assets found for the first 3 prefixes in data/noble_gas_compounds/")

    # Setup data source JSON for test
    test_source_json = tmp_path / "test_noble_gas_source.json"
    test_source_json.write_text(json.dumps(test_data))
    
    # Destination dir for downloads
    dest_dir = tmp_path / "downloads_real"
    dest_dir.mkdir()
    
    # Run download_omol25.py
    test_args = [
        "download_omol25",
        "--data-source", str(test_source_json),
        "--local-dir", str(mock_s3_dir)
    ]
    
    monkeypatch.chdir(dest_dir)
    with patch.object(sys, "argv", test_args):
        download_main()
        
    # Verify extraction
    for p in test_prefixes:
        # Check some key files that should be extracted
        # orca.tar.zst -> orca.out
        # orca.gbw.zstd0 -> orca.gbw
        # density_mat.npz -> density_mat.npz
        assert (dest_dir / p / "orca.out").exists()
        assert (dest_dir / p / "orca.gbw").exists()
        assert (dest_dir / p / "density_mat.npz").exists()
        
        # Verify sizes are non-zero (real data!)
        assert (dest_dir / p / "orca.out").stat().st_size > 0
        assert (dest_dir / p / "density_mat.npz").stat().st_size > 1000 # npz is usually large
        
    # Verify restart file
    restart_file = test_source_json.with_name(test_source_json.stem + "_restart.json")
    assert restart_file.exists()
    with open(restart_file, "r") as f:
        restart_data = json.load(f)
    for p in test_prefixes:
        assert restart_data[p]["processed"] is True
