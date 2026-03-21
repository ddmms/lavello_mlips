import subprocess
import sys
import json
import tarfile
import io
from pathlib import Path
from zstandard import ZstdCompressor

def create_mock_data(local_dir: Path, data_source: Path):
    """Creates a mock data structure on disk for testing."""
    local_dir.mkdir(parents=True, exist_ok=True)
    with open(data_source, "r") as f:
        data = json.load(f)
    
    # Minimal ORCA output that passes the parser
    mock_orca_out = """
    -------------------------------------------------------------------------------
                                     ORCA OUTPUT
    -------------------------------------------------------------------------------
    Number of atoms .............................. 2
    * xyz 0 1
    H 0.0 0.0 0.0
    H 0.0 0.0 0.74
    *
    -----------------------
    CARTESIAN COORDINATES (ANGSTROEM)
    -----------------------
      H      0.000000    0.000000    0.000000
      H      0.000000    0.000000    0.740000

    Total Charge           : 0
    Multiplicity           : 1
    Total Dipole Moment (a.u.) : 0.0 0.0 0.0
    -----------------------
    EIGENVALUES
    -----------------------
    NO  OCC          E(Eh)            E(eV) 
     0  2.0000      -0.5000         -13.6057
     1  0.0000       0.2000           5.4423
    -------------------------
    ****ORCA TERMINATED NORMALLY****
    """
    mock_property = "Total Dipole Moment (a.u.) : 0.0 0.0 0.0"
    mock_orca_inp = "* xyz 0 1"

    c = ZstdCompressor()
    
    for prefix, info in data.items():
        key = info['key']
        # If key is string, it's a tar.zst. If list, handle each.
        keys = [key] if isinstance(key, str) else key
        
        for k in keys:
            source_path = local_dir / prefix / k
            source_path.parent.mkdir(parents=True, exist_ok=True)
            
            if k.endswith(".tar.zst"):
                tar_buffer = io.BytesIO()
                with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
                    for name, content in [("orca.out", mock_orca_out), 
                                         ("orca.inp", mock_orca_inp), 
                                         ("orca.property.txt", mock_property)]:
                        info_tar = tarfile.TarInfo(name=name)
                        info_tar.size = len(content)
                        tar.addfile(info_tar, io.BytesIO(content.encode('utf-8')))
                
                compressed = c.compress(tar_buffer.getvalue())
                source_path.write_bytes(compressed)
            else:
                # Just write a dummy file for non-tar keys if any
                source_path.write_bytes(b"dummy")

def test_process_omol25_mpi():
    out_dir = Path("test_output_dir")
    local_data_dir = Path("mock_s3_data")
    test_data_source = Path("test_noble_gas_prefix.json")
    
    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob('*'), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()
    
    # Setup
    Path(test_data_source).write_bytes(Path("data/noble_gas_compounds_prefix.json").read_bytes())
    create_mock_data(local_data_dir, test_data_source)
    
    cmd = [
        "mpirun", "--oversubscribe", "-n", "2", 
        sys.executable, "-m", "process_omol25.cli",
        "--login-file", "psdi-argonne-omol25-ro.json", # Still needed but dummy
        "--data-source", str(test_data_source),
        "--output-dir", str(out_dir),
        "--local-dir", str(local_data_dir),
        "--mpi"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed: {result.stderr}"
    
    expected_out = out_dir / "props_test_noble_gas.parquet"
    assert expected_out.exists()
    
    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob('*'), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()
    test_data_source.unlink(missing_ok=True)
    Path("test_noble_gas_prefix_restart.json").unlink(missing_ok=True)


def test_process_omol25_no_mpi():
    out_dir = Path("test_output_dir_no_mpi")
    local_data_dir = Path("mock_s3_data_no_mpi")
    test_data_source = Path("test_noble_gas_prefix_no_mpi.json")
    
    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob('*'), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()
            
    # Setup
    Path(test_data_source).write_bytes(Path("data/noble_gas_compounds_prefix.json").read_bytes())
    create_mock_data(local_data_dir, test_data_source)
    
    cmd = [
        sys.executable, "-m", "process_omol25.cli",
        "--login-file", "psdi-argonne-omol25-ro.json",
        "--data-source", str(test_data_source),
        "--output-dir", str(out_dir),
        "--local-dir", str(local_data_dir)
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed: {result.stderr}"
    
    expected_out = out_dir / "props_test_noble_gas_no_mpi.parquet"
    assert expected_out.exists()
    
    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob('*'), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()
    test_data_source.unlink(missing_ok=True)
    Path("test_noble_gas_prefix_no_mpi_restart.json").unlink(missing_ok=True)


def test_download_omol25():
    local_data_dir = Path("mock_s3_data_download")
    test_data_source = Path("test_download_prefix.json")
    
    # Cleanup
    if local_data_dir.exists():
        for p in sorted(local_data_dir.rglob('*'), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
        local_data_dir.rmdir()

    # Setup
    Path(test_data_source).write_bytes(Path("data/noble_gas_compounds_prefix.json").read_bytes())
    create_mock_data(local_data_dir, test_data_source)
    
    cmd = [
        sys.executable, "-m", "process_omol25.download_omol25",
        "--login-file", "psdi-argonne-omol25-ro.json",
        "--data-source", str(test_data_source),
        "--local-dir", str(local_data_dir),
        "--sample-size", "1"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed: {result.stderr}"
    
    # Check if a file was extracted. 
    expected_file = Path("noble_gas_compounds/FXeNSO2F2_step20_0_1/orca.out")
    assert expected_file.exists()
    
    # Cleanup
    if local_data_dir.exists():
        for p in sorted(local_data_dir.rglob('*'), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
        local_data_dir.rmdir()
    
    with open(test_data_source, "r") as f:
        data = json.load(f)
    for prefix in data:
        p = Path(prefix)
        if p.exists():
            for sub in sorted(p.rglob('*'), reverse=True):
                sub.unlink() if sub.is_file() else sub.rmdir()
            try:
                p.rmdir()
            except OSError:
                pass # Might not be empty if multiple tests ran
            
    test_data_source.unlink(missing_ok=True)
    Path("test_download_prefix_restart.json").unlink(missing_ok=True)


def test_process_omol25_restart_mpi():
    out_dir = Path("test_output_restart_mpi")
    local_data_dir = Path("mock_s3_data_restart")
    test_data_source = Path("test_restart_prefix.json")
    restart_file = Path("test_restart_prefix_restart.json")
    
    # Cleanup
    for p in [out_dir, local_data_dir, test_data_source, restart_file]:
        if p.exists():
            if p.is_dir():
                for sub in sorted(p.rglob('*'), reverse=True):
                    sub.unlink() if sub.is_file() else sub.rmdir()
                p.rmdir()
            else:
                p.unlink()
    
    # Setup
    Path(test_data_source).write_bytes(Path("data/noble_gas_compounds_prefix.json").read_bytes())
    create_mock_data(local_data_dir, test_data_source)
    
    # Run 1: process only 2 samples
    cmd1 = [
        "mpirun", "--oversubscribe", "-n", "2",
        sys.executable, "-m", "process_omol25.cli",
        "--login-file", "psdi-argonne-omol25-ro.json",
        "--data-source", str(test_data_source),
        "--output-dir", str(out_dir),
        "--local-dir", str(local_data_dir),
        "--sample-size", "2",
        "--mpi"
    ]
    subprocess.run(cmd1, capture_output=True, text=True)
    
    # Check restart file: at least some should be marked processed
    assert restart_file.exists()
    with open(restart_file, "r") as f:
        data = json.load(f)
    processed_count = sum(1 for v in data.values() if v.get("processed"))
    assert processed_count == 2
    
    # Run 2: resume remaining samples
    cmd2 = [
        "mpirun", "--oversubscribe", "-n", "2",
        sys.executable, "-m", "process_omol25.cli",
        "--login-file", "psdi-argonne-omol25-ro.json",
        "--data-source", str(restart_file),
        "--output-dir", str(out_dir),
        "--local-dir", str(local_data_dir),
        "--restart",
        "--mpi"
    ]
    result = subprocess.run(cmd2, capture_output=True, text=True)
    assert result.returncode == 0
    
    # Final check: all should be processed in the restart file
    with open(restart_file, "r") as f:
        data = json.load(f)
    assert all(v.get("processed") for v in data.values())
    
    # Cleanup
    for p in [out_dir, local_data_dir, test_data_source, restart_file]:
        if p.exists():
            if p.is_dir():
                for sub in sorted(p.rglob('*'), reverse=True):
                    sub.unlink() if sub.is_file() else sub.rmdir()
                p.rmdir()
            else:
                p.unlink()
