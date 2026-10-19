import json
import subprocess

import numpy as np
import pandas as pd
from ase import Atoms
from ase.io import write


def test_verify_processed_omol25_success(tmp_path):
    pq_file = tmp_path / "test_success.parquet"
    xyz_file = tmp_path / "test_success.xyz"

    # Simulate a successful matched dataset
    df = pd.DataFrame(
        [
            {
                "geom_sha1": "hash_valid1",
                "test_energy": -50.123,
                "argonne_rel": "path_1",
            },
            {
                "geom_sha1": "hash_valid2",
                "test_energy": -60.456,
                "process_time_s": 1.23,
                "argonne_rel": "path_2",
            },
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "hash_valid1"
    a1.info["test_energy"] = -50.123
    a1.info["argonne_rel"] = "path_1"

    a2 = Atoms("O2", positions=[(0, 0, 0), (0, 0, 1.2)])
    a2.info["geom_sha1"] = "hash_valid2"
    a2.info["test_energy"] = -60.456
    a2.info["argonne_rel"] = "path_2"
    # Note: process_time_s is not stored in XYZ, but we added a global default skip-keys for it

    write(str(xyz_file), [a1, a2], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, (
        f"Verification falsely failed:\n{res.stderr}\n{res.stdout}"
    )
    assert "Data Consistency Verified" in res.stdout
    assert "0 mismatches" in res.stdout


def test_verify_processed_omol25_structural_mismatch(tmp_path):
    pq_file = tmp_path / "test_struct_mismatch.parquet"
    xyz_file = tmp_path / "test_struct_mismatch.xyz"

    # Simulate a dataset where XYZ is missing a structure
    df = pd.DataFrame(
        [
            {"geom_sha1": "hash_miss1", "test_energy": -50.0, "argonne_rel": "path_1"},
            {"geom_sha1": "hash_miss2", "test_energy": -60.0},
        ]
    )
    df.to_parquet(pq_file)

    # XYZ only contains one element
    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "hash_miss1"
    a1.info["test_energy"] = -50.0
    a1.info["argonne_rel"] = "path_1"
    write(str(xyz_file), [a1], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "1 structures found in Parquet but missing from ExtXYZ" in res.stdout


def test_verify_processed_omol25_property_mismatch(tmp_path):
    pq_file = tmp_path / "test_prop_mismatch.parquet"
    xyz_file = tmp_path / "test_prop_mismatch.xyz"

    # Simulate a dataset where one numerical value does not match exactly
    df = pd.DataFrame(
        [{"geom_sha1": "hash_prop_err", "dipole": 0.123456789, "argonne_rel": "path_1"}]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "hash_prop_err"
    a1.info["dipole"] = 0.123450000  # Notice the tiny difference
    a1.info["argonne_rel"] = "path_1"
    write(str(xyz_file), [a1], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "Verification Failed" in res.stdout
    assert "1 property inconsistencies" in res.stdout


def test_verify_processed_omol25_skips_timing_keys(tmp_path):
    pq_file = tmp_path / "test_time.parquet"
    xyz_file = tmp_path / "test_time.xyz"

    # Simulate a dataset with a custom timing key
    df = pd.DataFrame(
        [
            {
                "geom_sha1": "hash_time",
                "test_energy": -50.0,
                "argonne_rel": "path_1",
                "custom_time_elapsed": 5.4,  # Matches "time" filter
            }
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "hash_time"
    a1.info["test_energy"] = -50.0
    a1.info["argonne_rel"] = "path_1"
    # XYZ naturally doesn't have custom timings, but the script should skip checking it because of "time" in the string!
    write(str(xyz_file), [a1], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, (
        f"Verification falsely failed on timing key:\n{res.stdout}\n{res.stderr}"
    )
    assert "Data Consistency Verified" in res.stdout


def test_verify_processed_omol25_array_properties_match(tmp_path):
    pq_file = tmp_path / "test_arrays_match.parquet"
    xyz_file = tmp_path / "test_arrays_match.xyz"

    df = pd.DataFrame(
        [
            {
                "geom_sha1": "hash_arr1",
                "argonne_rel": "path_arr_1",
                "dipole_vector": [0.0, 1.2, -0.5],
                "quadrupole_matrix": [[1.0, 0.0, 0.0], [0.0, -0.5, 0.0], [0.0, 0.0, -0.5]],
            }
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "hash_arr1"
    a1.info["argonne_rel"] = "path_arr_1"
    a1.info["dipole_vector"] = np.array([0.0, 1.2, -0.5])
    a1.info["quadrupole_matrix"] = np.array([[1.0, 0.0, 0.0], [0.0, -0.5, 0.0], [0.0, 0.0, -0.5]])
    write(str(xyz_file), [a1], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, (
        f"Verification failed with array properties:\n{res.stdout}\n{res.stderr}"
    )
    assert "Data Consistency Verified" in res.stdout
    assert "0 mismatches" in res.stdout


def test_verify_processed_omol25_array_properties_mismatch(tmp_path):
    pq_file = tmp_path / "test_arrays_mismatch.parquet"
    xyz_file = tmp_path / "test_arrays_mismatch.xyz"

    df = pd.DataFrame(
        [
            {
                "geom_sha1": "hash_arr2",
                "argonne_rel": "path_arr_2",
                "dipole_vector": [0.0, 1.2, -0.5],
            }
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "hash_arr2"
    a1.info["argonne_rel"] = "path_arr_2"
    a1.info["dipole_vector"] = np.array([0.0, 1.2, 0.5])  # sign difference
    write(str(xyz_file), [a1], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "Verification Failed" in res.stdout
    assert "1 property inconsistencies" in res.stdout


def test_verify_processed_omol25_with_json_success(tmp_path):
    pq_file = tmp_path / "test_json_success.parquet"
    xyz_file = tmp_path / "test_json_success.xyz"
    json_file = tmp_path / "test_json_success.json"

    json_data = {
        "ani2x/mol_1/": {"key": "orca.tar.zst", "processed": True},
        "ani2x/mol_2/": {"key": "orca.tar.zst", "processed": False},
    }
    json_file.write_text(json.dumps(json_data))

    df = pd.DataFrame(
        [
            {"geom_sha1": "sha_1", "test_energy": -10.0, "argonne_rel": "ani2x/mol_1/"},
            {"geom_sha1": "sha_2", "test_energy": -20.0, "argonne_rel": "ani2x/mol_2/"},
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "sha_1"
    a1.info["test_energy"] = -10.0
    a1.info["argonne_rel"] = "ani2x/mol_1/"

    a2 = Atoms("O2", positions=[(0, 0, 0), (0, 0, 1.2)])
    a2.info["geom_sha1"] = "sha_2"
    a2.info["test_energy"] = -20.0
    a2.info["argonne_rel"] = "ani2x/mol_2/"

    write(str(xyz_file), [a1, a2], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
        "--json",
        str(json_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Command failed:\nSTDOUT: {res.stdout}\nSTDERR: {res.stderr}"
    assert "Alignment with JSON" in res.stdout
    assert "All 2 keys in JSON match Parquet records" in res.stdout
    assert "All 2 keys in JSON match ExtXYZ structures" in res.stdout
    assert "JSON Alignment perfect: All 2 keys correspond across JSON, Parquet, and ExtXYZ" in res.stdout
    assert "Data Consistency Verified" in res.stdout


def test_verify_processed_omol25_with_json_slash_normalization(tmp_path):
    pq_file = tmp_path / "test_json_slash.parquet"
    xyz_file = tmp_path / "test_json_slash.xyz"
    json_file = tmp_path / "test_json_slash.json"

    # JSON keys have trailing slashes
    json_data = {
        "ani2x/mol_1/": {"key": "orca.tar.zst"},
        "ani2x/mol_2/": {"key": "orca.tar.zst"},
    }
    json_file.write_text(json.dumps(json_data))

    # Parquet and XYZ lack trailing slashes
    df = pd.DataFrame(
        [
            {"geom_sha1": "sha_1", "test_energy": -10.0, "argonne_rel": "ani2x/mol_1"},
            {"geom_sha1": "sha_2", "test_energy": -20.0, "argonne_rel": "ani2x/mol_2"},
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "sha_1"
    a1.info["test_energy"] = -10.0
    a1.info["argonne_rel"] = "ani2x/mol_1"

    a2 = Atoms("O2", positions=[(0, 0, 0), (0, 0, 1.2)])
    a2.info["geom_sha1"] = "sha_2"
    a2.info["test_energy"] = -20.0
    a2.info["argonne_rel"] = "ani2x/mol_2"

    write(str(xyz_file), [a1, a2], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
        "--data-source",
        str(json_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Command failed:\nSTDOUT: {res.stdout}\nSTDERR: {res.stderr}"
    assert "Parquet records match JSON keys when ignoring trailing slashes" in res.stdout
    assert "ExtXYZ structures match JSON keys when ignoring trailing slashes" in res.stdout
    assert "JSON Alignment perfect" in res.stdout


def test_verify_processed_omol25_with_json_mismatch(tmp_path):
    pq_file = tmp_path / "test_json_mismatch.parquet"
    xyz_file = tmp_path / "test_json_mismatch.xyz"
    json_file = tmp_path / "test_json_mismatch.json"

    # JSON has mol_1 and mol_missing
    json_data = {
        "ani2x/mol_1/": {"key": "orca.tar.zst"},
        "ani2x/mol_missing/": {"key": "orca.tar.zst"},
    }
    json_file.write_text(json.dumps(json_data))

    # Parquet and ExtXYZ have mol_1 and mol_extra
    df = pd.DataFrame(
        [
            {"geom_sha1": "sha_1", "test_energy": -10.0, "argonne_rel": "ani2x/mol_1/"},
            {"geom_sha1": "sha_extra", "test_energy": -30.0, "argonne_rel": "ani2x/mol_extra/"},
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "sha_1"
    a1.info["test_energy"] = -10.0
    a1.info["argonne_rel"] = "ani2x/mol_1/"

    a_extra = Atoms("O2", positions=[(0, 0, 0), (0, 0, 1.2)])
    a_extra.info["geom_sha1"] = "sha_extra"
    a_extra.info["test_energy"] = -30.0
    a_extra.info["argonne_rel"] = "ani2x/mol_extra/"

    write(str(xyz_file), [a1, a_extra], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
        "--json",
        str(json_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "1 keys found in JSON but missing from Parquet" in res.stdout
    assert "1 records found in Parquet but missing from JSON" in res.stdout
    assert "1 keys found in JSON but missing from ExtXYZ" in res.stdout
    assert "1 structures found in ExtXYZ but missing from JSON" in res.stdout
    assert "JSON Alignment failed" in res.stdout


def test_verify_processed_omol25_with_json_file_not_found(tmp_path):
    pq_file = tmp_path / "dummy.parquet"
    xyz_file = tmp_path / "dummy.xyz"
    pq_file.write_text("")
    xyz_file.write_text("")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
        "--json",
        str(tmp_path / "non_existent.json"),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode != 0
    assert "does not exist" in (res.stderr + res.stdout)


def test_verify_json_contains_more_keys_than_parquet_and_extxyz(tmp_path):
    pq_file = tmp_path / "test_json_surplus.parquet"
    xyz_file = tmp_path / "test_json_surplus.xyz"
    json_file = tmp_path / "test_json_surplus.json"

    # JSON has 3 keys
    json_data = {
        "dataset/mol_1/": {"key": "orca.tar.zst"},
        "dataset/mol_2/": {"key": "orca.tar.zst"},
        "dataset/mol_3/": {"key": "orca.tar.zst"},
    }
    json_file.write_text(json.dumps(json_data))

    # Parquet and XYZ only have 2 keys
    df = pd.DataFrame(
        [
            {"geom_sha1": "sha_1", "test_energy": -10.0, "argonne_rel": "dataset/mol_1/"},
            {"geom_sha1": "sha_2", "test_energy": -20.0, "argonne_rel": "dataset/mol_2/"},
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "sha_1"
    a1.info["test_energy"] = -10.0
    a1.info["argonne_rel"] = "dataset/mol_1/"

    a2 = Atoms("O2", positions=[(0, 0, 0), (0, 0, 1.2)])
    a2.info["geom_sha1"] = "sha_2"
    a2.info["test_energy"] = -20.0
    a2.info["argonne_rel"] = "dataset/mol_2/"

    write(str(xyz_file), [a1, a2], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
        "--json",
        str(json_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "JSON file contains more keys than Parquet and ExtXYZ: 3 in JSON vs 2 in Parquet and 2 in ExtXYZ" in res.stdout
    assert "JSON file contains more keys than Parquet records: 3 JSON keys vs 2 Parquet records (1 surplus keys in JSON)" in res.stdout
    assert "JSON file contains more keys than ExtXYZ structures: 3 JSON keys vs 2 ExtXYZ structures (1 surplus keys in JSON)" in res.stdout


def test_verify_same_sha_different_species_not_duplicate(tmp_path):
    pq_file = tmp_path / "test_diff_species.parquet"
    xyz_file = tmp_path / "test_diff_species.xyz"

    # Same SHA, but different atom species (H2 vs O2)
    df = pd.DataFrame(
        [
            {"geom_sha1": "sha_shared", "test_energy": -10.0, "argonne_rel": "mol_h2", "total_charge_e": 0},
            {"geom_sha1": "sha_shared", "test_energy": -20.0, "argonne_rel": "mol_o2", "total_charge_e": 0},
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "sha_shared"
    a1.info["test_energy"] = -10.0
    a1.info["argonne_rel"] = "mol_h2"
    a1.info["total_charge_e"] = 0

    a2 = Atoms("O2", positions=[(0, 0, 0), (0, 0, 1)])
    a2.info["geom_sha1"] = "sha_shared"
    a2.info["test_energy"] = -20.0
    a2.info["argonne_rel"] = "mol_o2"
    a2.info["total_charge_e"] = 0

    write(str(xyz_file), [a1, a2], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "differing in atomic species or charge; not duplicates" in res.stdout
    assert "Confirmed duplicate structures detected" not in res.stdout


def test_verify_same_sha_same_species_different_charge_not_duplicate(tmp_path):
    pq_file = tmp_path / "test_diff_charge.parquet"
    xyz_file = tmp_path / "test_diff_charge.xyz"

    # Same SHA, same species (H2O), but different charge states (0 vs +1)
    df = pd.DataFrame(
        [
            {"geom_sha1": "sha_shared_geom", "test_energy": -10.0, "argonne_rel": "mol_neutral", "total_charge_e": 0},
            {"geom_sha1": "sha_shared_geom", "test_energy": -20.0, "argonne_rel": "mol_cation", "total_charge_e": 1},
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2O", positions=[(0, 0, 0), (0, 1, 0), (1, 0, 0)])
    a1.info["geom_sha1"] = "sha_shared_geom"
    a1.info["test_energy"] = -10.0
    a1.info["argonne_rel"] = "mol_neutral"
    a1.info["total_charge_e"] = 0

    a2 = Atoms("H2O", positions=[(0, 0, 0), (0, 1, 0), (1, 0, 0)])
    a2.info["geom_sha1"] = "sha_shared_geom"
    a2.info["test_energy"] = -20.0
    a2.info["argonne_rel"] = "mol_cation"
    a2.info["total_charge_e"] = 1

    write(str(xyz_file), [a1, a2], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "differing in atomic species or charge; not duplicates" in res.stdout
    assert "Confirmed duplicate structures detected" not in res.stdout


def test_verify_same_sha_same_species_same_charge_confirmed_duplicate(tmp_path):
    pq_file = tmp_path / "test_dup.parquet"
    xyz_file = tmp_path / "test_dup.xyz"

    # Same SHA, same species (H2O), same charge (0), but different argonne_rel
    df = pd.DataFrame(
        [
            {"geom_sha1": "sha_duplicate", "test_energy": -10.0, "argonne_rel": "mol_dup_1", "total_charge_e": 0},
            {"geom_sha1": "sha_duplicate", "test_energy": -10.0, "argonne_rel": "mol_dup_2", "total_charge_e": 0},
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2O", positions=[(0, 0, 0), (0, 1, 0), (1, 0, 0)])
    a1.info["geom_sha1"] = "sha_duplicate"
    a1.info["test_energy"] = -10.0
    a1.info["argonne_rel"] = "mol_dup_1"
    a1.info["total_charge_e"] = 0

    a2 = Atoms("H2O", positions=[(0, 0, 0), (0, 1, 0), (1, 0, 0)])
    a2.info["geom_sha1"] = "sha_duplicate"
    a2.info["test_energy"] = -10.0
    a2.info["argonne_rel"] = "mol_dup_2"
    a2.info["total_charge_e"] = 0

    write(str(xyz_file), [a1, a2], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "Confirmed duplicate structures detected" in res.stdout
    assert "Structural redundancy detected" in res.stdout

    # Verify duplicate dump file was created in parquet parent dir
    dup_file = tmp_path / "duplicates_test_dup.json"
    assert dup_file.exists()
    dup_data = json.loads(dup_file.read_text())
    assert "sha_duplicate" in dup_data["confirmed_duplicates"]
    assert len(dup_data["confirmed_duplicates"]["sha_duplicate"]) == 2


def test_verify_parquet_has_data_xyz_missing_with_restart_status(tmp_path):
    pq_file = tmp_path / "test_data_in_pq.parquet"
    xyz_file = tmp_path / "test_data_in_pq.xyz"
    json_file = tmp_path / "test_data_in_pq_restart.json"

    json_data = {
        "rgd_uks/mol_1/": {"processed": True, "key": "orca.tar.zst"}
    }
    json_file.write_text(json.dumps(json_data))

    df = pd.DataFrame(
        [
            {
                "geom_sha1": "sha_1",
                "argonne_rel": "rgd_uks/mol_1/",
                "spin_quadrupole_traceless": [[1.0, 0.0, 0.0], [0.0, -0.5, 0.0], [0.0, 0.0, -0.5]],
            }
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "sha_1"
    a1.info["argonne_rel"] = "rgd_uks/mol_1/"
    # ExtXYZ is missing spin_quadrupole_traceless
    write(str(xyz_file), [a1], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
        "--json",
        str(json_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "Data IS PRESENT in Parquet" in res.stdout
    assert "spin_quadrupole_traceless" in res.stdout
    assert "key is MISSING from ExtXYZ `atoms.info`" in res.stdout
    assert "[Restart status: processed=True]" in res.stdout
    assert "data present in Parquet but missing from ExtXYZ `atoms.info`" in res.stdout


def test_verify_parquet_nan_xyz_missing_is_not_mismatch(tmp_path):
    pq_file = tmp_path / "test_null_match.parquet"
    xyz_file = tmp_path / "test_null_match.xyz"

    # Mol 1 has spin_quadrupole_traceless, Mol 2 does not (NaN in Parquet)
    df = pd.DataFrame(
        [
            {
                "geom_sha1": "sha_1",
                "argonne_rel": "mol_1",
                "test_energy": -10.0,
                "spin_quadrupole_traceless": [[1.0, 0.0], [0.0, 1.0]],
            },
            {
                "geom_sha1": "sha_2",
                "argonne_rel": "mol_2",
                "test_energy": -20.0,
                "spin_quadrupole_traceless": None,  # NaN in Parquet
            },
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "sha_1"
    a1.info["argonne_rel"] = "mol_1"
    a1.info["test_energy"] = -10.0
    a1.info["spin_quadrupole_traceless"] = np.array([[1.0, 0.0], [0.0, 1.0]])

    a2 = Atoms("O2", positions=[(0, 0, 0), (0, 0, 1.2)])
    a2.info["geom_sha1"] = "sha_2"
    a2.info["argonne_rel"] = "mol_2"
    a2.info["test_energy"] = -20.0
    # a2 has no spin_quadrupole_traceless in info

    write(str(xyz_file), [a1, a2], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0
    assert "Data Consistency Verified" in res.stdout
    assert "0 mismatches" in res.stdout


def test_verify_xyz_has_data_parquet_missing(tmp_path):
    pq_file = tmp_path / "test_xyz_extra.parquet"
    xyz_file = tmp_path / "test_xyz_extra.xyz"

    df = pd.DataFrame(
        [
            {
                "geom_sha1": "sha_1",
                "argonne_rel": "mol_1",
            }
        ]
    )
    df.to_parquet(pq_file)

    a1 = Atoms("H2", positions=[(0, 0, 0), (0, 0, 1)])
    a1.info["geom_sha1"] = "sha_1"
    a1.info["argonne_rel"] = "mol_1"
    a1.info["extra_property"] = 42.0
    write(str(xyz_file), [a1], format="extxyz")

    cmd = [
        "verify_processed_omol25",
        "--parquet",
        str(pq_file),
        "--extxyz",
        str(xyz_file),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert "Data IS PRESENT in ExtXYZ `atoms.info`" in res.stdout
    assert "extra_property" in res.stdout
    assert "key is MISSING from Parquet columns" in res.stdout


def test_verify_omol_with_start_index_and_sample_processed_data(tmp_path):
    """
    Test that running process_omol with --start-index and --sample-size produces
    chunked files, and verify_processed_omol25 (and verify_omol) with --start-index
    and --sample verifies them successfully.
    """
    from unittest.mock import patch
    try:
        from test_process_omol25 import create_mock_data
    except ImportError:
        from tests.test_process_omol25 import create_mock_data
    from lavello_mlips.process_omol25 import OmolDataProcessor

    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    local_data_dir = tmp_path / "mock_s3"
    data_source = tmp_path / "omol_prefix.json"

    # Create dataset with 6 molecules
    prefixes = [f"mol_{i}/" for i in range(6)]
    data = {p: {"key": "data.tar.zst", "processed": False} for p in prefixes}
    with open(data_source, "w", encoding="utf-8") as f:
        json.dump(data, f)

    create_mock_data(local_data_dir, data_source)

    class Args:
        def __init__(self, start_idx, sample_sz):
            self.data_source = data_source
            self.output_dir = output_dir
            self.local_dir = local_data_dir
            self.login_file = None
            self.bucket = None
            self.start_index = start_idx
            self.sample_size = sample_sz
            self.restart = False
            self.batch_size = 2
            self.memory_threshold_gb = 10.0
            self.mpi = False
            self.log_file = None

    # Run process_omol on chunk [0, 3]
    args1 = Args(0, 3)
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        proc1 = OmolDataProcessor(args1, rank=0, size=1, comm=None)
        assert proc1.group_name == "omol_0_3"
        proc1.run_serial()

    pq1 = output_dir / "props_omol_0_3.parquet"
    xyz1 = output_dir / "structs_omol_0_3.xyz"
    assert pq1.exists() and xyz1.exists()

    # 1. Verify with explicit parquet and extxyz using verify_processed_omol25
    cmd1 = [
        "verify_processed_omol25",
        "--parquet", str(pq1),
        "--extxyz", str(xyz1),
        "--data-source", str(data_source),
        "--start-index", "0",
        "--sample", "3",
    ]
    res1 = subprocess.run(cmd1, capture_output=True, text=True)
    assert res1.returncode == 0, f"Verification failed:\n{res1.stdout}\n{res1.stderr}"
    assert "Data Consistency Verified! 0 mismatches found across 3 shared structures." in res1.stdout

    # 2. Verify with auto-discovery (omitting --parquet and --extxyz) using verify_omol script
    cmd2 = [
        "verify_omol",
        "--data-source", str(data_source),
        "--start-index", "0",
        "--sample", "3",
        "--output-dir", str(output_dir),
    ]
    res2 = subprocess.run(cmd2, capture_output=True, text=True)
    assert res2.returncode == 0, f"Auto-discovery verification failed:\n{res2.stdout}\n{res2.stderr}"
    assert "Data Consistency Verified! 0 mismatches found across 3 shared structures." in res2.stdout

    # Run process_omol on chunk [3, 6]
    args2 = Args(3, 3)
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        proc2 = OmolDataProcessor(args2, rank=0, size=1, comm=None)
        assert proc2.group_name == "omol_3_6"
        proc2.run_serial()

    pq2 = output_dir / "props_omol_3_6.parquet"
    xyz2 = output_dir / "structs_omol_3_6.xyz"
    assert pq2.exists() and xyz2.exists()

    # Verify chunk [3, 6] using verify_omol
    cmd3 = [
        "verify_omol",
        "--data-source", str(data_source),
        "--start-index", "3",
        "--sample-size", "3",
        "--output-dir", str(output_dir),
    ]
    res3 = subprocess.run(cmd3, capture_output=True, text=True)
    assert res3.returncode == 0, f"Chunk [3, 6] verification failed:\n{res3.stdout}\n{res3.stderr}"
    assert "Data Consistency Verified! 0 mismatches found across 3 shared structures." in res3.stdout


def test_verify_omol_slice_of_full_dataset(tmp_path):
    """
    Test verify_omol with --start-index and --sample on a full dataset (Parquet and ExtXYZ).
    Only the requested slice should be validated.
    """
    pq_file = tmp_path / "full_props.parquet"
    xyz_file = tmp_path / "full_structs.xyz"
    json_file = tmp_path / "full_prefix.json"

    # 5 molecules in total
    prefixes = [f"full_mol_{i}/" for i in range(5)]
    json_data = {p: {"processed": True} for p in prefixes}
    json_file.write_text(json.dumps(json_data))

    rows = []
    atoms_list = []
    for i, p in enumerate(prefixes):
        rows.append({
            "geom_sha1": f"sha_{i}",
            "argonne_rel": p,
            "test_prop": float(i * 10),
        })
        at = Atoms("H2", positions=[(0, 0, 0), (0, 0, 0.74 + i * 0.01)])
        at.info["geom_sha1"] = f"sha_{i}"
        at.info["argonne_rel"] = p
        at.info["test_prop"] = float(i * 10)
        atoms_list.append(at)

    pd.DataFrame(rows).to_parquet(pq_file)
    write(str(xyz_file), atoms_list, format="extxyz")

    # Verify slice [1, 3] (2 items: mol_1, mol_2)
    cmd = [
        "verify_omol",
        "--parquet", str(pq_file),
        "--extxyz", str(xyz_file),
        "--data-source", str(json_file),
        "--start-index", "1",
        "--sample", "2",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, f"Failed:\n{res.stdout}\n{res.stderr}"
    assert "Data Consistency Verified! 0 mismatches found across 2 shared structures." in res.stdout


def test_verify_omol_slice_mismatch_detected(tmp_path):
    """
    Test that property mismatch in a specific slice is detected when included,
    but ignored when the slice does not include that record.
    """
    pq_file = tmp_path / "slice_err_props.parquet"
    xyz_file = tmp_path / "slice_err_structs.xyz"
    json_file = tmp_path / "slice_err_prefix.json"

    prefixes = [f"err_mol_{i}/" for i in range(4)]
    json_data = {p: {"processed": True} for p in prefixes}
    json_file.write_text(json.dumps(json_data))

    rows = []
    atoms_list = []
    for i, p in enumerate(prefixes):
        rows.append({
            "geom_sha1": f"sha_err_{i}",
            "argonne_rel": p,
            "test_energy": -10.0 * i,
        })
        at = Atoms("H2", positions=[(0, 0, 0), (0, 0, 0.74)])
        at.info["geom_sha1"] = f"sha_err_{i}"
        at.info["argonne_rel"] = p
        # Record 2 has corrupted test_energy in XYZ
        at.info["test_energy"] = 999.0 if i == 2 else -10.0 * i
        atoms_list.append(at)

    pd.DataFrame(rows).to_parquet(pq_file)
    write(str(xyz_file), atoms_list, format="extxyz")

    # Slice [0, 2] does NOT include index 2 -> should pass!
    cmd_pass = [
        "verify_omol",
        "--parquet", str(pq_file),
        "--extxyz", str(xyz_file),
        "--data-source", str(json_file),
        "--start-index", "0",
        "--sample", "2",
    ]
    res_pass = subprocess.run(cmd_pass, capture_output=True, text=True)
    assert res_pass.returncode == 0
    assert "Data Consistency Verified! 0 mismatches found across 2 shared structures." in res_pass.stdout

    # Slice [2, 4] DOES include index 2 -> should detect mismatch!
    cmd_fail = [
        "verify_omol",
        "--parquet", str(pq_file),
        "--extxyz", str(xyz_file),
        "--data-source", str(json_file),
        "--start-index", "2",
        "--sample", "2",
    ]
    res_fail = subprocess.run(cmd_fail, capture_output=True, text=True)
    assert "Verification Failed" in res_fail.stdout
    assert "Numeric mismatch: Parquet=-20.0 != XYZ=999.0" in res_fail.stdout


def test_cli_and_download_support_sample_flag(tmp_path):
    """Test that --sample flag is accepted by CLI parsers as alias for --sample-size."""
    from unittest.mock import patch
    from lavello_mlips.cli import parse_args as cli_parse_args
    from lavello_mlips.download_omol25 import parse_args as dl_parse_args

    test_args_cli = ["--data-source", "test.json", "--sample", "42"]
    with patch("sys.argv", ["lavello_mlips"] + test_args_cli):
        parsed = cli_parse_args()
        assert parsed.sample_size == 42

    test_args_dl = ["--data-source", "test.json", "--sample", "24"]
    with patch("sys.argv", ["download_omol25"] + test_args_dl):
        parsed_dl = dl_parse_args()
        assert parsed_dl.sample_size == 24




