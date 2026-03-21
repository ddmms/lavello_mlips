import subprocess
import sys
from pathlib import Path

def rmtree(path: Path):
    if path.exists():
        for p in sorted(path.rglob('*'), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
        path.rmdir()

def test_process_omol25_mpi():
    out_dir = Path("test_output_dir")
    test_data = Path("test_noble_gas_prefix.json")
    
    rmtree(out_dir)
    
    test_data.write_bytes(Path("data/noble_gas_compounds_prefix.json").read_bytes())
    
    cli_path = Path(sys.executable).parent / "process_omol25"
    cmd = [
        "mpirun", "--oversubscribe", "-n", "2", 
        str(cli_path),
        "--login-file", "psdi-argonne-omol25-ro.json",
        "--data-source", str(test_data),
        "--output-dir", str(out_dir),
        "--mpi"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    assert result.returncode == 0, f"Command failed with return code {result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    
    expected_out = out_dir / "props_test_noble_gas.parquet"
    assert expected_out.exists(), f"Expected output {expected_out} not found!"
    
    # Cleanup
    rmtree(out_dir)
    test_data.unlink(missing_ok=True)
    Path("test_noble_gas_prefix_restart.json").unlink(missing_ok=True)


def test_process_omol25_no_mpi():
    out_dir = Path("test_output_dir_no_mpi")
    test_data = Path("test_noble_gas_prefix_no_mpi.json")
    
    rmtree(out_dir)
    
    test_data.write_bytes(Path("data/noble_gas_compounds_prefix.json").read_bytes())
    
    cli_path = Path(sys.executable).parent / "process_omol25"
    cmd = [
        str(cli_path),
        "--login-file", "psdi-argonne-omol25-ro.json",
        "--data-source", str(test_data),
        "--output-dir", str(out_dir)
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    assert result.returncode == 0, f"Command failed with return code {result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    
    expected_out = out_dir / "props_test_noble_gas_no_mpi.parquet"
    assert expected_out.exists(), f"Expected output {expected_out} not found!"
    
    # Cleanup
    rmtree(out_dir)
    test_data.unlink(missing_ok=True)
    Path("test_noble_gas_prefix_no_mpi_restart.json").unlink(missing_ok=True)
