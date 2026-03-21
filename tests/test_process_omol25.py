import subprocess
import os
import shutil
import sys

def test_process_omol25_mpi():
    out_dir = "test_output_dir"
    test_data = "test_noble_gas_prefix.json"
    
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
        
    shutil.copy("data/noble_gas_compounds_prefix.json", test_data)
    
    cli_path = os.path.join(os.path.dirname(sys.executable), "process_omol25")
    cmd = [
        "mpirun", "-n", "2", 
        cli_path,
        "--login-file", "psdi-argonne-omol25-ro.json",
        "--data-source", test_data,
        "--output-dir", out_dir,
        "--mpi"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    assert result.returncode == 0, f"Command failed with return code {result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    
    # stem is test_noble_gas_prefix -> replaces "_prefix" -> test_noble_gas
    expected_out = f"{out_dir}/props_test_noble_gas.parquet"
    assert os.path.exists(expected_out), f"Expected output {expected_out} not found!"
    
    # Cleanup
    shutil.rmtree(out_dir)
    os.remove(test_data)
    if os.path.exists("test_noble_gas_prefix_restart.json"):
        os.remove("test_noble_gas_prefix_restart.json")


def test_process_omol25_no_mpi():
    out_dir = "test_output_dir_no_mpi"
    test_data = "test_noble_gas_prefix_no_mpi.json"
    
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
        
    shutil.copy("data/noble_gas_compounds_prefix.json", test_data)
    
    cli_path = os.path.join(os.path.dirname(sys.executable), "process_omol25")
    cmd = [
        cli_path,
        "--login-file", "psdi-argonne-omol25-ro.json",
        "--data-source", test_data,
        "--output-dir", out_dir
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    assert result.returncode == 0, f"Command failed with return code {result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    
    expected_out = f"{out_dir}/props_test_noble_gas_no_mpi.parquet"
    assert os.path.exists(expected_out), f"Expected output {expected_out} not found!"
    
    # Cleanup
    shutil.rmtree(out_dir)
    os.remove(test_data)
    if os.path.exists("test_noble_gas_prefix_no_mpi_restart.json"):
        os.remove("test_noble_gas_prefix_no_mpi_restart.json")
