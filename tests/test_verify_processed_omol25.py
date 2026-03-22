import subprocess
import pandas as pd
from ase import Atoms
from ase.io import write

def test_verify_processed_omol25_success(tmp_path):
    pq_file = tmp_path / "test_success.parquet"
    xyz_file = tmp_path / "test_success.xyz"

    # Simulate a successful matched dataset
    df = pd.DataFrame([
        {"geom_sha1": "hash_valid1", "test_energy": -50.123, "argonne_rel": "path_1"},
        {"geom_sha1": "hash_valid2", "test_energy": -60.456, "process_time_s": 1.23, "argonne_rel": "path_2"}
    ])
    df.to_parquet(pq_file)

    a1 = Atoms('H2', positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "hash_valid1"
    a1.info["test_energy"] = -50.123
    a1.info["argonne_rel"] = "path_1"

    a2 = Atoms('O2', positions=[(0, 0, 0), (0, 0, 1.2)])
    a2.info["geom_sha1"] = "hash_valid2"
    a2.info["test_energy"] = -60.456
    a2.info["argonne_rel"] = "path_2"
    # Note: process_time_s is not stored in XYZ, but we added a global default skip-keys for it

    write(str(xyz_file), [a1, a2], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet", str(pq_file),
        "--extxyz", str(xyz_file)
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Verification falsely failed:\n{res.stderr}\n{res.stdout}"
    assert "Data Consistency Verified" in res.stdout
    assert "0 mismatches" in res.stdout

def test_verify_processed_omol25_structural_mismatch(tmp_path):
    pq_file = tmp_path / "test_struct_mismatch.parquet"
    xyz_file = tmp_path / "test_struct_mismatch.xyz"

    # Simulate a dataset where XYZ is missing a structure
    df = pd.DataFrame([
        {"geom_sha1": "hash_miss1", "test_energy": -50.0, "argonne_rel": "path_1"},
        {"geom_sha1": "hash_miss2", "test_energy": -60.0}
    ])
    df.to_parquet(pq_file)

    # XYZ only contains one element
    a1 = Atoms('H2', positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "hash_miss1"
    a1.info["test_energy"] = -50.0
    a1.info["argonne_rel"] = "path_1"
    write(str(xyz_file), [a1], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet", str(pq_file),
        "--extxyz", str(xyz_file)
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "1 structures found in Parquet but missing from ExtXYZ" in res.stdout

def test_verify_processed_omol25_property_mismatch(tmp_path):
    pq_file = tmp_path / "test_prop_mismatch.parquet"
    xyz_file = tmp_path / "test_prop_mismatch.xyz"

    # Simulate a dataset where one numerical value does not match exactly
    df = pd.DataFrame([
        {"geom_sha1": "hash_prop_err", "dipole": 0.123456789, "argonne_rel": "path_1"}
    ])
    df.to_parquet(pq_file)

    a1 = Atoms('H2', positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "hash_prop_err"
    a1.info["dipole"] = 0.123450000  # Notice the tiny difference
    a1.info["argonne_rel"] = "path_1"
    write(str(xyz_file), [a1], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet", str(pq_file),
        "--extxyz", str(xyz_file)
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "Verification Failed" in res.stdout
    assert "1 property inconsistencies" in res.stdout

def test_verify_processed_omol25_skips_timing_keys(tmp_path):
    pq_file = tmp_path / "test_time.parquet"
    xyz_file = tmp_path / "test_time.xyz"

    # Simulate a dataset with a custom timing key
    df = pd.DataFrame([
        {
            "geom_sha1": "hash_time",
            "test_energy": -50.0,
            "argonne_rel": "path_1",
            "custom_time_elapsed": 5.4  # Matches "time" filter
        }
    ])
    df.to_parquet(pq_file)

    a1 = Atoms('H2', positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "hash_time"
    a1.info["test_energy"] = -50.0
    a1.info["argonne_rel"] = "path_1"
    # XYZ naturally doesn't have custom timings, but the script should skip checking it because of "time" in the string!
    write(str(xyz_file), [a1], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet", str(pq_file),
        "--extxyz", str(xyz_file)
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Verification falsely failed on timing key:\n{res.stdout}\n{res.stderr}"
    assert "Data Consistency Verified" in res.stdout
