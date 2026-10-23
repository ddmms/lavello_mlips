import io
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import tarfile
from unittest.mock import MagicMock, patch

from ase import Atoms
import ase.io
import numpy as np
import pandas as pd
from zstandard import ZstdCompressor

from lavello_mlips.process_omol25 import OmolDataProcessor


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
        key = info["key"]
        # If key is string, it's a tar.zst. If list, handle each.
        keys = [key] if isinstance(key, str) else key

        for k in keys:
            source_path = local_dir / prefix / k
            source_path.parent.mkdir(parents=True, exist_ok=True)

            if k.endswith(".tar.zst"):
                tar_buffer = io.BytesIO()
                with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
                    for name, content in [
                        ("orca.out", mock_orca_out),
                        ("orca.inp", mock_orca_inp),
                        ("orca.property.txt", mock_property),
                    ]:
                        info_tar = tarfile.TarInfo(name=name)
                        info_tar.size = len(content)
                        tar.addfile(info_tar, io.BytesIO(content.encode("utf-8")))

                compressed = c.compress(tar_buffer.getvalue())
                source_path.write_bytes(compressed)
            else:
                # Just write a dummy file for non-tar keys if any
                source_path.write_bytes(b"dummy")


def test_lavello_mlips_mpi():
    out_dir = Path("test_output_dir")
    local_data_dir = Path("mock_s3_data")
    test_data_source = Path("test_noble_gas_prefix.json")

    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob("*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()

    # Setup
    Path(test_data_source).write_bytes(
        Path("data/noble_gas_compounds_prefix.json").read_bytes()
    )
    create_mock_data(local_data_dir, test_data_source)

    cmd = [
        "mpirun",
        "--oversubscribe",
        "-n",
        "2",
        sys.executable,
        "-m",
        "lavello_mlips.cli",
        "--data-source",
        str(test_data_source),
        "--output-dir",
        str(out_dir),
        "--local-dir",
        str(local_data_dir),
        "--mpi",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with {result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"

    expected_out = out_dir / "props_test_noble_gas.parquet"
    assert expected_out.exists()

    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob("*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()
    test_data_source.unlink(missing_ok=True)
    Path("test_noble_gas_prefix_restart.json").unlink(missing_ok=True)


def test_lavello_mlips_no_mpi():
    out_dir = Path("test_output_dir_no_mpi")
    local_data_dir = Path("mock_s3_data_no_mpi")
    test_data_source = Path("test_noble_gas_prefix_no_mpi.json")

    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob("*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()

    # Setup
    Path(test_data_source).write_bytes(
        Path("data/noble_gas_compounds_prefix.json").read_bytes()
    )
    create_mock_data(local_data_dir, test_data_source)

    cmd = [
        sys.executable,
        "-m",
        "lavello_mlips.cli",
        "--data-source",
        str(test_data_source),
        "--output-dir",
        str(out_dir),
        "--local-dir",
        str(local_data_dir),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with {result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"

    expected_out = out_dir / "props_test_noble_gas_no_mpi.parquet"
    assert expected_out.exists()

    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob("*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()
    test_data_source.unlink(missing_ok=True)
    Path("test_noble_gas_prefix_no_mpi_restart.json").unlink(missing_ok=True)


def test_download_omol25():
    local_data_dir = Path("mock_s3_data_download")
    test_data_source = Path("test_download_prefix.json")

    # Cleanup
    if local_data_dir.exists():
        for p in sorted(local_data_dir.rglob("*"), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
        local_data_dir.rmdir()

    # Setup
    Path(test_data_source).write_bytes(
        Path("data/noble_gas_compounds_prefix.json").read_bytes()
    )
    create_mock_data(local_data_dir, test_data_source)

    cmd = [
        sys.executable,
        "-m",
        "lavello_mlips.download_omol25",
        "--data-source",
        str(test_data_source),
        "--local-dir",
        str(local_data_dir),
        "--sample-size",
        "1",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with {result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"

    # Check if a file was extracted.
    expected_file = Path("noble_gas_compounds/FXeNSO2F2_step20_0_1/orca.out")
    assert expected_file.exists()

    # Cleanup
    if local_data_dir.exists():
        for p in sorted(local_data_dir.rglob("*"), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
        local_data_dir.rmdir()

    with open(test_data_source, "r") as f:
        data = json.load(f)
    for prefix in data:
        p = Path(prefix)
        if p.exists():
            for sub in sorted(p.rglob("*"), reverse=True):
                sub.unlink() if sub.is_file() else sub.rmdir()
            try:
                p.rmdir()
            except OSError:
                pass  # Might not be empty if multiple tests ran

    test_data_source.unlink(missing_ok=True)
    Path("test_download_prefix_restart.json").unlink(missing_ok=True)


def test_lavello_mlips_restart_mpi():
    out_dir = Path("test_output_restart_mpi")
    local_data_dir = Path("mock_s3_data_restart")
    test_data_source = Path("test_restart_prefix.json")
    restart_file = Path("test_restart_prefix_restart.json")

    # Cleanup
    for p in [out_dir, local_data_dir, test_data_source, restart_file]:
        if p.exists():
            if p.is_dir():
                for sub in sorted(p.rglob("*"), reverse=True):
                    sub.unlink() if sub.is_file() else sub.rmdir()
                p.rmdir()
            else:
                p.unlink()

    # Setup
    Path(test_data_source).write_bytes(
        Path("data/noble_gas_compounds_prefix.json").read_bytes()
    )
    create_mock_data(local_data_dir, test_data_source)

    # Run 1: process only 2 samples
    cmd1 = [
        "mpirun",
        "--oversubscribe",
        "-n",
        "2",
        sys.executable,
        "-m",
        "lavello_mlips.cli",
        "--data-source",
        str(test_data_source),
        "--output-dir",
        str(out_dir),
        "--local-dir",
        str(local_data_dir),
        "--sample-size",
        "2",
        "--mpi",
    ]
    result1 = subprocess.run(cmd1, capture_output=True, text=True)
    assert result1.returncode == 0, f"MPI run 1 failed with {result1.returncode}:\nSTDOUT: {result1.stdout}\nSTDERR: {result1.stderr}"

    # Check restart file: at least some should be marked processed
    assert restart_file.exists()
    with open(restart_file, "r") as f:
        data = json.load(f)
    processed_count = sum(1 for v in data.values() if v.get("processed"))
    assert processed_count == 2

    # Run 2: resume remaining samples
    cmd2 = [
        "mpirun",
        "--oversubscribe",
        "-n",
        "2",
        sys.executable,
        "-m",
        "lavello_mlips.cli",
        "--data-source",
        str(restart_file),
        "--output-dir",
        str(out_dir),
        "--local-dir",
        str(local_data_dir),
        "--restart",
        "--mpi",
    ]
    result = subprocess.run(cmd2, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with {result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"

    # Final check: all should be processed in the restart file
    with open(restart_file, "r") as f:
        data = json.load(f)
    assert all(v.get("processed") for v in data.values())

    # Cleanup
    for p in [out_dir, local_data_dir, test_data_source, restart_file]:
        if p.exists():
            if p.is_dir():
                for sub in sorted(p.rglob("*"), reverse=True):
                    sub.unlink() if sub.is_file() else sub.rmdir()
                p.rmdir()
            else:
                p.unlink()


def test_extxyz_props_consistency():
    """Verify that every property in the Parquet row matches atoms.info in the .xyz output,
    linked via geom_sha1 as the common key."""
    out_dir = Path("test_output_extxyz")
    local_data_dir = Path("mock_s3_data_extxyz")
    test_data_source = Path("test_extxyz_prefix.json")

    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob("*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()
    test_data_source.unlink(missing_ok=True)
    Path("test_extxyz_prefix_restart.json").unlink(missing_ok=True)

    # Setup: use the small prefix fixture
    Path(test_data_source).write_bytes(
        Path("data/noble_gas_compounds_prefix.json").read_bytes()
    )
    create_mock_data(local_data_dir, test_data_source)

    # Run serial (no mpirun) to get a single pair of output files
    cmd = [
        sys.executable,
        "-m",
        "lavello_mlips.cli",
        "--data-source",
        str(test_data_source),
        "--output-dir",
        str(out_dir),
        "--local-dir",
        str(local_data_dir),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Serial run failed with {result.returncode}:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"

    # After merge there should be exactly ONE Parquet and ONE XYZ file (no rank-specific parts)
    parquet_files = list(out_dir.glob("props_*.parquet"))
    xyz_files = list(out_dir.glob("structs_*.xyz"))
    assert len(parquet_files) == 1, f"Expected 1 merged Parquet, got: {parquet_files}"
    assert len(xyz_files) == 1, f"Expected 1 merged XYZ, got: {xyz_files}"
    # Confirm the merged names have no rank/part suffix (no underscore-digit pattern after group name)
    for f in parquet_files + xyz_files:
        assert not re.search(r"_\d+(_part_\d+)?\.(parquet|xyz)$", f.name), (
            f"Part/rank file was not merged: {f.name}"
        )

    # Load Parquet; index by geom_sha1
    df = pd.read_parquet(parquet_files[0])
    assert "geom_sha1" in df.columns, "geom_sha1 missing from Parquet"
    parquet_by_sha = {row["geom_sha1"]: row for _, row in df.iterrows()}

    # Load atoms from the single merged XYZ file
    all_atoms = ase.io.read(str(xyz_files[0]), index=":")
    assert all_atoms, "No atoms read from ExtXYZ file"

    xyz_by_sha = {}
    for at in all_atoms:
        sha = at.info.get("geom_sha1")
        assert sha is not None, "geom_sha1 missing from an atoms.info entry"
        xyz_by_sha[sha] = at

    # Every Parquet SHA must match an XYZ entry and vice versa
    assert set(parquet_by_sha.keys()) == set(xyz_by_sha.keys()), (
        "geom_sha1 keys differ between Parquet and XYZ outputs:\n"
        f"  Parquet-only: {set(parquet_by_sha) - set(xyz_by_sha)}\n"
        f"  XYZ-only:     {set(xyz_by_sha) - set(parquet_by_sha)}"
    )

    # Properties that are not written into atoms.info or are processing metadata
    skip_keys = {"process_time_s"}

    for sha, row in parquet_by_sha.items():
        info = xyz_by_sha[sha].info
        for key, pq_val in row.items():
            if key in skip_keys or key not in info:
                continue
            xyz_val = info[key]
            pq_is_nan = pq_val is None or (
                isinstance(pq_val, float) and math.isnan(pq_val)
            )
            xyz_is_nan = xyz_val is None or (
                isinstance(xyz_val, float) and math.isnan(xyz_val)
            )
            if pq_is_nan:
                assert xyz_is_nan, (
                    f"sha={sha} key={key}: Parquet=NaN/None but XYZ={xyz_val!r}"
                )
            else:
                assert pq_val == xyz_val, (
                    f"sha={sha} key={key}: Parquet={pq_val!r} != XYZ={xyz_val!r}"
                )

    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob("*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()
    test_data_source.unlink(missing_ok=True)
    Path("test_extxyz_prefix_restart.json").unlink(missing_ok=True)


def test_restart_5ranks_matches_serial():
    """Verify that a two-phase MPI restart run (5 ranks, split halfway) produces
    the exact same Parquet rows and XYZ structures as a single serial run,
    matching each structure via geom_sha1."""
    # ---- shared helpers ----
    def cleanup(*paths):
        for p in paths:
            if p.exists():
                if p.is_dir():
                    for sub in sorted(p.rglob("*"), reverse=True):
                        sub.unlink() if sub.is_file() else sub.rmdir()
                    p.rmdir()
                else:
                    p.unlink()

    # ---- paths ----
    serial_dir = Path("test_restart5_serial_out")
    mpi_dir = Path("test_restart5_mpi_out")
    local_data_dir = Path("mock_s3_data_restart5")
    data_source = Path("test_restart5_prefix.json")
    restart_file = Path("test_restart5_prefix_restart.json")

    cleanup(serial_dir, mpi_dir, local_data_dir, data_source, restart_file)

    # ---- setup: use the small prefix fixture (3 entries) ----
    data_source.write_bytes(Path("data/noble_gas_compounds_prefix.json").read_bytes())
    create_mock_data(local_data_dir, data_source)

    with open(data_source) as f:
        n_total = len(json.load(f))
    half = n_total // 2  # first MPI pass processes this many
    _rest = n_total - half  # second MPI pass (restart) picks up the remainder

    # ======================================================
    # 1. Serial run – ground truth
    # ======================================================
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "lavello_mlips.cli",
            "--data-source",
            str(data_source),
            "--output-dir",
            str(serial_dir),
            "--local-dir",
            str(local_data_dir),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Serial run failed with {result.returncode}:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"

    serial_parquet = list(serial_dir.glob("props_*.parquet"))
    serial_xyz = list(serial_dir.glob("structs_*.xyz"))
    assert len(serial_parquet) == 1, f"Expected 1 merged Parquet, got {serial_parquet}"
    assert len(serial_xyz) == 1, f"Expected 1 merged XYZ, got {serial_xyz}"

    serial_df = pd.read_parquet(serial_parquet[0])
    serial_ats = ase.io.read(str(serial_xyz[0]), index=":")
    serial_pq_by_sha = {r["geom_sha1"]: r for _, r in serial_df.iterrows()}
    serial_xyz_by_sha = {at.info["geom_sha1"]: at for at in serial_ats}

    # ======================================================
    # 2. MPI restart run in two phases
    # ======================================================
    base_cmd = [
        "mpirun",
        "--oversubscribe",
        "-n",
        "5",
        sys.executable,
        "-m",
        "lavello_mlips.cli",
        "--output-dir",
        str(mpi_dir),
        "--local-dir",
        str(local_data_dir),
        "--mpi",
    ]

    # Phase 1: process first `half` items
    r1 = subprocess.run(
        base_cmd + ["--data-source", str(data_source), "--sample-size", str(half)],
        capture_output=True,
        text=True,
    )
    assert r1.returncode == 0, f"MPI phase-1 failed with {r1.returncode}:\nSTDOUT: {r1.stdout}\nSTDERR: {r1.stderr}"
    assert restart_file.exists(), "Restart file not created after phase 1"

    # Phase 2: continue from restart file (picks up remaining items)
    r2 = subprocess.run(
        base_cmd + ["--data-source", str(restart_file), "--restart"],
        capture_output=True,
        text=True,
    )
    assert r2.returncode == 0, f"MPI phase-2 failed with {r2.returncode}:\nSTDOUT: {r2.stdout}\nSTDERR: {r2.stderr}"

    mpi_parquet = list(mpi_dir.glob("props_*.parquet"))
    mpi_xyz = list(mpi_dir.glob("structs_*.xyz"))
    assert len(mpi_parquet) == 1, f"Expected 1 merged Parquet, got {mpi_parquet}"
    assert len(mpi_xyz) == 1, f"Expected 1 merged XYZ, got {mpi_xyz}"

    mpi_df = pd.read_parquet(mpi_parquet[0])
    mpi_ats = ase.io.read(str(mpi_xyz[0]), index=":")
    mpi_pq_by_sha = {r["geom_sha1"]: r for _, r in mpi_df.iterrows()}
    mpi_xyz_by_sha = {at.info["geom_sha1"]: at for at in mpi_ats}

    # ======================================================
    # 3. Compare serial vs MPI-restart
    # ======================================================
    assert set(serial_pq_by_sha) == set(mpi_pq_by_sha), (
        "Parquet SHA sets differ between serial and MPI-restart runs\n"
        f"  serial-only: {set(serial_pq_by_sha) - set(mpi_pq_by_sha)}\n"
        f"  mpi-only:    {set(mpi_pq_by_sha) - set(serial_pq_by_sha)}"
    )
    assert set(serial_xyz_by_sha) == set(mpi_xyz_by_sha), (
        "XYZ SHA sets differ between serial and MPI-restart runs"
    )

    # argonne_rel and data_id are source-path metadata, not physical properties;
    # they can legitimately differ when the same geometry appears under different prefixes
    # (as in mock data where all structures are identical H2 molecules).
    skip_keys = {"process_time_s", "argonne_rel", "data_id"}
    for sha in serial_pq_by_sha:
        s_row = serial_pq_by_sha[sha]
        m_row = mpi_pq_by_sha[sha]
        for key in s_row.index:
            if key in skip_keys:
                continue
            sv, mv = s_row[key], m_row[key]
            s_nan = sv is None or (isinstance(sv, float) and math.isnan(sv))
            m_nan = mv is None or (isinstance(mv, float) and math.isnan(mv))
            if s_nan:
                assert m_nan, f"sha={sha} key={key}: serial=NaN but mpi={mv!r}"
            else:
                assert sv == mv, f"sha={sha} key={key}: serial={sv!r} != mpi={mv!r}"

        # XYZ info comparison
        s_info = serial_xyz_by_sha[sha].info
        m_info = mpi_xyz_by_sha[sha].info
        for key in s_info:
            if key in skip_keys or key not in m_info:
                continue
            sv, mv = s_info[key], m_info[key]
            s_nan = sv is None or (isinstance(sv, float) and math.isnan(sv))
            m_nan = mv is None or (isinstance(mv, float) and math.isnan(mv))
            if s_nan:
                assert m_nan, f"sha={sha} xyz key={key}: serial=NaN but mpi={mv!r}"
            else:
                assert sv == mv, f"sha={sha} xyz key={key}: serial={sv!r} != mpi={mv!r}"

    # ---- cleanup ----
    cleanup(serial_dir, mpi_dir, local_data_dir, data_source, restart_file)


def test_download_omol25_mpi():
    out_dir = Path("test_output_dir_download_mpi")
    local_data_dir = Path("mock_s3_data_download_mpi")
    test_data_source = Path("test_download_prefix_mpi.json")

    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob("*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()

    # Setup
    Path(test_data_source).write_bytes(
        Path("data/noble_gas_compounds_prefix.json").read_bytes()
    )
    create_mock_data(local_data_dir, test_data_source)

    cmd = [
        "mpirun",
        "--oversubscribe",
        "-n",
        "2",
        sys.executable,
        "-m",
        "lavello_mlips.download_omol25",
        "--data-source",
        str(test_data_source),
        "--local-dir",
        str(local_data_dir),
        "--sample-size",
        "2",
        "--mpi",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"Command failed with {result.returncode}.\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"

    # Check if a file was extracted.
    expected_file = Path("noble_gas_compounds/FXeNSO2F2_step20_0_1/orca.out")
    assert expected_file.exists()

    # Check restart file:
    restart_file = Path("test_download_prefix_mpi_restart.json")
    assert restart_file.exists(), f"Restart file {restart_file} was not created"

    # Cleanup
    for d in [out_dir, local_data_dir]:
        if d.exists():
            for p in sorted(d.rglob("*"), reverse=True):
                p.unlink() if p.is_file() else p.rmdir()
            d.rmdir()
    test_data_source.unlink(missing_ok=True)
    restart_file.unlink(missing_ok=True)


def test_final_merge_corrupt_part_file_resets_restart_json(tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    data_source = tmp_path / "merge_test_prefix.json"
    restart_file = tmp_path / "merge_test_prefix_restart.json"

    data = {
        "struct_1": {"key": "s1.tar.zst", "processed": True},
        "struct_2": {"key": "s2.tar.zst", "processed": True},
        "struct_3": {"key": "s3.tar.zst", "processed": True},
    }
    with open(data_source, "w") as f:
        json.dump(data, f)

    class MockArgs:
        def __init__(self):
            self.data_source = data_source
            self.output_dir = output_dir
            self.bucket = None
            self.local_dir = None
            self.login_file = None
            self.sample_size = -1
            self.batch_size = 100
            self.start_index = 0
            self.restart = False
            self.mpi = False
            self.memory_threshold_gb = 10.0

    args = MockArgs()
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        processor = OmolDataProcessor(args, rank=0, size=1, comm=None)

    with open(restart_file, "w") as f:
        json.dump(data, f)

    df0 = pd.DataFrame([
        {"argonne_rel": "struct_1", "data_id": 1, "energy": -1.0},
        {"argonne_rel": "struct_2", "data_id": 2, "energy": -2.0},
    ])
    df0.to_parquet(output_dir / "props_merge_test_0_part_0.parquet", index=False)
    at1 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at1.info = {"argonne_rel": "struct_1", "data_id": 1}
    at2 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.75]])
    at2.info = {"argonne_rel": "struct_2", "data_id": 2}
    ase.io.write(str(output_dir / "structs_merge_test_0_part_0.xyz"), [at1, at2], format="extxyz")

    corrupt_pq = output_dir / "props_merge_test_1_part_0.parquet"
    corrupt_pq.write_bytes(b"corrupt parquet bytes")
    at3 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.76]])
    at3.info = {"argonne_rel": "struct_3", "data_id": 3}
    ase.io.write(str(output_dir / "structs_merge_test_1_part_0.xyz"), [at3], format="extxyz")

    processor._final_merge(1.0)

    assert restart_file.exists()
    with open(restart_file, "r") as f:
        saved_data = json.load(f)
    assert saved_data["struct_1"]["processed"] is True
    assert saved_data["struct_2"]["processed"] is True
    assert saved_data["struct_3"]["processed"] is False

    final_pq = output_dir / "props_merge_test.parquet"
    final_xyz = output_dir / "structs_merge_test.xyz"
    assert final_pq.exists()
    assert final_xyz.exists()
    final_df = pd.read_parquet(final_pq)
    assert set(final_df["argonne_rel"]) == {"struct_1", "struct_2"}
    final_atoms = ase.io.read(str(final_xyz), index=":")
    assert {at.info["argonne_rel"] for at in final_atoms} == {"struct_1", "struct_2"}

    assert not corrupt_pq.exists()
    assert not (output_dir / "structs_merge_test_1_part_0.xyz").exists()


def test_final_merge_write_error_resets_restart_json(tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    data_source = tmp_path / "write_err_test_prefix.json"
    restart_file = tmp_path / "write_err_test_prefix_restart.json"

    data = {
        "struct_1": {"key": "s1.tar.zst", "processed": True},
    }
    with open(data_source, "w") as f:
        json.dump(data, f)

    class MockArgs:
        def __init__(self):
            self.data_source = data_source
            self.output_dir = output_dir
            self.bucket = None
            self.local_dir = None
            self.login_file = None
            self.sample_size = -1
            self.batch_size = 100
            self.start_index = 0
            self.restart = False
            self.mpi = False
            self.memory_threshold_gb = 10.0

    args = MockArgs()
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        processor = OmolDataProcessor(args, rank=0, size=1, comm=None)

    with open(restart_file, "w") as f:
        json.dump(data, f)

    df0 = pd.DataFrame([{"argonne_rel": "struct_1", "data_id": 1, "energy": -1.0}])
    df0.to_parquet(output_dir / "props_write_err_test_0_part_0.parquet", index=False)
    at1 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at1.info = {"argonne_rel": "struct_1", "data_id": 1}
    ase.io.write(str(output_dir / "structs_write_err_test_0_part_0.xyz"), [at1], format="extxyz")

    with patch.object(pd.DataFrame, "to_parquet", side_effect=IOError("Disk write failed")):
        processor._final_merge(1.0)

    with open(restart_file, "r") as f:
        saved_data = json.load(f)
    assert saved_data["struct_1"]["processed"] is False


def test_final_merge_mismatch_resets_restart_json(tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    data_source = tmp_path / "mismatch_test_prefix.json"
    restart_file = tmp_path / "mismatch_test_prefix_restart.json"

    data = {
        "struct_1": {"key": "s1.tar.zst", "processed": True},
        "struct_2": {"key": "s2.tar.zst", "processed": True},
    }
    with open(data_source, "w") as f:
        json.dump(data, f)

    class MockArgs:
        def __init__(self):
            self.data_source = data_source
            self.output_dir = output_dir
            self.bucket = None
            self.local_dir = None
            self.login_file = None
            self.sample_size = -1
            self.batch_size = 100
            self.start_index = 0
            self.restart = False
            self.mpi = False
            self.memory_threshold_gb = 10.0

    args = MockArgs()
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        processor = OmolDataProcessor(args, rank=0, size=1, comm=None)

    with open(restart_file, "w") as f:
        json.dump(data, f)

    df0 = pd.DataFrame([
        {"argonne_rel": "struct_1", "data_id": 1, "energy": -1.0},
        {"argonne_rel": "struct_2", "data_id": 2, "energy": -2.0},
    ])
    df0.to_parquet(output_dir / "props_mismatch_test_0_part_0.parquet", index=False)
    at1 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at1.info = {"argonne_rel": "struct_1", "data_id": 1}
    ase.io.write(str(output_dir / "structs_mismatch_test_0_part_0.xyz"), [at1], format="extxyz")

    processor._final_merge(1.0)

    with open(restart_file, "r") as f:
        saved_data = json.load(f)
    assert saved_data["struct_1"]["processed"] is True
    assert saved_data["struct_2"]["processed"] is False


def test_restart_recovers_intersection_on_length_mismatch(tmp_path):
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    data_source = tmp_path / "intersect_test_prefix.json"
    restart_file = tmp_path / "intersect_test_prefix_restart.json"

    data = {
        "struct_1": {"key": "s1.tar.zst", "processed": True},
        "struct_2": {"key": "s2.tar.zst", "processed": True},
        "struct_3": {"key": "s3.tar.zst", "processed": True},
        "struct_4": {"key": "s4.tar.zst", "processed": False},
    }
    with open(data_source, "w") as f:
        json.dump(data, f)
    with open(restart_file, "w") as f:
        json.dump(data, f)

    # Parquet has struct_1 and struct_2 (2 rows)
    # ExtXYZ has struct_1, struct_2, and struct_3 (3 structures)
    df = pd.DataFrame([
        {"argonne_rel": "struct_1", "data_id": 1, "energy": -1.0},
        {"argonne_rel": "struct_2", "data_id": 2, "energy": -2.0},
    ])
    df.to_parquet(output_dir / "props_intersect_test.parquet", index=False)

    at1 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at1.info = {"argonne_rel": "struct_1", "data_id": 1}
    at2 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.75]])
    at2.info = {"argonne_rel": "struct_2", "data_id": 2}
    at3 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.76]])
    at3.info = {"argonne_rel": "struct_3", "data_id": 3}
    ase.io.write(str(output_dir / "structs_intersect_test.xyz"), [at1, at2, at3], format="extxyz")

    class MockArgs:
        def __init__(self):
            self.data_source = restart_file
            self.output_dir = output_dir
            self.bucket = None
            self.local_dir = None
            self.login_file = None
            self.sample_size = -1
            self.batch_size = 100
            self.start_index = 0
            self.restart = True
            self.mpi = False
            self.memory_threshold_gb = 10.0

    args = MockArgs()
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        processor = OmolDataProcessor(args, rank=0, size=1, comm=None)

    pq_path = output_dir / "props_intersect_test.parquet"
    xyz_path = output_dir / "structs_intersect_test.xyz"
    assert pq_path.exists()
    assert xyz_path.exists()
    repaired_df = pd.read_parquet(pq_path)
    repaired_atoms = ase.io.read(str(xyz_path), index=":")
    assert len(repaired_df) == 2
    assert len(repaired_atoms) == 2
    assert set(repaired_df["argonne_rel"]) == {"struct_1", "struct_2"}
    assert {at.info["argonne_rel"] for at in repaired_atoms} == {"struct_1", "struct_2"}

    with open(restart_file, "r") as f:
        saved_data = json.load(f)
    assert saved_data["struct_1"]["processed"] is True
    assert saved_data["struct_2"]["processed"] is True
    assert saved_data["struct_3"]["processed"] is False
    assert saved_data["struct_4"]["processed"] is False

    assert set(processor.prefixes) == {"struct_3", "struct_4"}


def test_worker_logging_to_file(tmp_path):
    """Test in a subprocess that worker rank (> 0) writes warnings, errors, and captured warnings into the log file."""
    data_source = tmp_path / "worker_log_test.json"
    data_source.write_text("{}")
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    log_file = tmp_path / "custom_worker.log"

    script = f"""
import sys
import logging
import warnings
from unittest.mock import MagicMock, patch
from pathlib import Path
from lavello_mlips.process_omol25 import OmolDataProcessor

class MockArgs:
    def __init__(self):
        self.data_source = Path(r'{data_source}')
        self.output_dir = Path(r'{output_dir}')
        self.log_file = Path(r'{log_file}')
        self.bucket = None
        self.local_dir = None
        self.login_file = None
        self.sample_size = -1
        self.batch_size = 100
        self.start_index = 0
        self.restart = False
        self.mpi = True
        self.memory_threshold_gb = 10.0
        self.log_level = 'INFO'

args = MockArgs()
mock_comm = MagicMock()
mock_comm.Get_rank.return_value = 1
mock_comm.Get_size.return_value = 2

with patch('lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client', return_value=None):
    processor = OmolDataProcessor(args, rank=1, size=2, comm=mock_comm)

proc_logger = logging.getLogger('lavello_mlips.process_omol25')
proc_logger.warning('Test worker warning message')
proc_logger.error('Test worker error message')
warnings.warn('Test worker captured python warning')
"""
    cmd = [sys.executable, "-c", script]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0
    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")
    assert "Test worker warning message" in content
    assert "Test worker error message" in content
    assert "Test worker captured python warning" in content


def test_worker_uncaught_exception_logged(tmp_path):
    """Test in a subprocess that sys.excepthook logs uncaught exceptions to the log file."""
    data_source = tmp_path / "uncaught_test.json"
    data_source.write_text("{}")
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    log_file = tmp_path / "uncaught.log"

    script = f"""
import sys
from unittest.mock import MagicMock, patch
from pathlib import Path
from lavello_mlips.process_omol25 import OmolDataProcessor

class MockArgs:
    def __init__(self):
        self.data_source = Path(r'{data_source}')
        self.output_dir = Path(r'{output_dir}')
        self.log_file = Path(r'{log_file}')
        self.bucket = None
        self.local_dir = None
        self.login_file = None
        self.sample_size = -1
        self.batch_size = 100
        self.start_index = 0
        self.restart = False
        self.mpi = True
        self.memory_threshold_gb = 10.0
        self.log_level = 'INFO'

args = MockArgs()
mock_comm = MagicMock()
with patch('lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client', return_value=None):
    OmolDataProcessor(args, rank=1, size=2, comm=mock_comm)

raise RuntimeError('Fatal worker error simulated')
"""
    cmd = [sys.executable, "-c", script]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode != 0
    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")
    assert "Fatal worker error simulated" in content
    assert "CRITICAL" in content


def test_process_single_sets_processed_false_on_missing_file(tmp_path):
    """Test process_single sets processed=False when local file is missing."""
    prefix_json = tmp_path / "test_missing.json"
    prefix = "mol_missing/"
    data = {prefix: {"key": "orca.tar.zst", "processed": False}}
    prefix_json.write_text(json.dumps(data))

    class MockArgs:
        data_source = prefix_json
        output_dir = tmp_path / "out"
        bucket = "mock-bucket"
        local_dir = tmp_path / "data"  # empty dir
        login_file = None
        sample_size = -1
        batch_size = 10
        start_index = 0
        restart = False
        memory_threshold_gb = 1.0

    MockArgs.local_dir.mkdir(parents=True, exist_ok=True)
    MockArgs.output_dir.mkdir(parents=True, exist_ok=True)

    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        processor = OmolDataProcessor(MockArgs(), rank=0, size=1, comm=None)
        processor.data[prefix]["processed"] = True
        res = processor.process_single(0)
        assert res is None
        assert processor.data[prefix]["processed"] is False


def test_process_single_sets_processed_false_on_secondary_key_transfer_failure(tmp_path):
    """Test process_single sets processed=False when a secondary key in key_list fails to transfer."""
    local_data_dir = tmp_path / "data"
    prefix_json = tmp_path / "test_partial.json"
    prefix = "mol_partial/"
    # Key list includes orca.tar.zst and orca.gbw, but orca.gbw is missing
    data = {prefix: {"key": ["orca.tar.zst", "orca.gbw.zstd0"], "processed": False}}
    prefix_json.write_text(json.dumps(data))

    create_mock_data(local_data_dir, prefix_json)
    # Delete the secondary key to simulate transfer failure
    gbw_path = local_data_dir / prefix / "orca.gbw.zstd0"
    if gbw_path.exists():
        gbw_path.unlink()

    class MockArgs:
        data_source = prefix_json
        output_dir = tmp_path / "out"
        bucket = "mock-bucket"
        local_dir = local_data_dir
        login_file = None
        sample_size = -1
        batch_size = 10
        start_index = 0
        restart = False
        memory_threshold_gb = 1.0

    MockArgs.output_dir.mkdir(parents=True, exist_ok=True)

    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        processor = OmolDataProcessor(MockArgs(), rank=0, size=1, comm=None)
        processor.data[prefix]["processed"] = True
        res = processor.process_single(0)
        assert res is None
        assert processor.data[prefix]["processed"] is False


def test_process_single_sets_processed_false_on_s3_error(tmp_path):
    """Test process_single sets processed=False on S3 get_object failure."""
    prefix_json = tmp_path / "test_s3_err.json"
    prefix = "mol_s3_err/"
    data = {prefix: {"key": "orca.tar.zst", "processed": False}}
    prefix_json.write_text(json.dumps(data))

    class MockArgs:
        data_source = prefix_json
        output_dir = tmp_path / "out"
        bucket = "mock-bucket"
        local_dir = None
        login_file = None
        sample_size = -1
        batch_size = 10
        start_index = 0
        restart = False
        memory_threshold_gb = 1.0

    MockArgs.output_dir.mkdir(parents=True, exist_ok=True)

    mock_s3 = MagicMock()
    mock_s3.get_object.side_effect = Exception("S3 Connection Dropped")

    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=mock_s3):
        processor = OmolDataProcessor(MockArgs(), rank=0, size=1, comm=None)
        processor.data[prefix]["processed"] = True
        res = processor.process_single(0, s3_client=mock_s3)
        assert res is None
        assert processor.data[prefix]["processed"] is False


def test_serial_marks_failed_record_processed_false_in_restart(tmp_path):
    """Test that serial execution marks failed records as processed=False in restart file."""
    prefix_json = tmp_path / "test_serial_fail.json"
    prefix = "mol_serial_fail/"
    data = {prefix: {"key": "missing.tar.zst", "processed": False}}
    prefix_json.write_text(json.dumps(data))

    class MockArgs:
        data_source = prefix_json
        output_dir = tmp_path / "out"
        bucket = "mock-bucket"
        local_dir = tmp_path / "empty_dir"
        login_file = None
        sample_size = -1
        batch_size = 10
        start_index = 0
        restart = False
        mpi = False
        memory_threshold_gb = 1.0

    MockArgs.local_dir.mkdir(parents=True, exist_ok=True)
    MockArgs.output_dir.mkdir(parents=True, exist_ok=True)

    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        processor = OmolDataProcessor(MockArgs(), rank=0, size=1, comm=None)
        processor.data[prefix]["processed"] = True
        processor.run_serial()

    restart_file = prefix_json.with_name(prefix_json.stem + "_restart.json")
    with open(restart_file, "r") as f:
        restart_data = json.load(f)
    assert restart_data[prefix]["processed"] is False



def test_integrals_from_orca_2json_cleans_up_shm_tempdir(tmp_path, monkeypatch):
    """Test that temporary files for orca_2json are created in /dev/shm (or mock) and deleted when done."""
    from lavello_mlips.process_omol25 import _integrals_from_orca_2json

    mock_shm = tmp_path / "dev_shm"
    mock_shm.mkdir()

    # Point get_shm_temp_dir to mock_shm
    monkeypatch.setattr("lavello_mlips.process_omol25.get_shm_temp_dir", lambda: str(mock_shm))

    created_dirs = []

    def mock_subprocess_run(cmd, cwd, capture_output, text):
        created_dirs.append(Path(cwd))
        # Ensure temporary files exist inside cwd while subprocess runs
        assert (Path(cwd) / "orca.gbw").exists()
        assert (Path(cwd) / "orca.SHARKINP.tmp").exists()
        assert (Path(cwd) / "orca.json.conf").exists()
        # Verify that cwd is indeed located within mock_shm
        assert str(mock_shm) in cwd

        # Simulate orca_2json creating orca.json
        payload = {
            "Molecule": {
                "S-Matrix": [[1.0]],
                "dipole": [[[0.0]], [[0.0]], [[0.0]]],
                "quadrupole": [[[0.0]], [[0.0]], [[0.0]], [[0.0]], [[0.0]], [[0.0]]],
            }
        }
        (Path(cwd) / "orca.json").write_text(json.dumps(payload))
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    # Dummy 1-atom setup with mock pointers
    import struct
    dummy_gbw = bytearray(b"\x00" * 2048)
    struct.pack_into("<q", dummy_gbw, 0, 16)  # calculation path at offset 16

    atoms = [(1, np.array([0.0, 0.0, 0.0]))]
    shells = [(0, 0, 0, np.array([1.0]), np.array([1.0]))]

    with patch("lavello_mlips.process_omol25._orca_2json_executable", return_value="orca_2json"), \
         patch("subprocess.run", side_effect=mock_subprocess_run):
        overlap, dipole, quadrupole = _integrals_from_orca_2json(bytes(dummy_gbw), atoms, shells)

    assert overlap.shape == (1, 1)
    assert len(created_dirs) == 1
    # Verify that the temporary directory was completely deleted after processing!
    assert not created_dirs[0].exists()


def test_unique_temp_prefix_mpi(tmp_path, monkeypatch):
    """Test that temporary prefixes in /dev/shm are unique per rank and process."""
    import os
    from lavello_mlips.utils import get_unique_temp_prefix
    from lavello_mlips.process_omol25 import _integrals_from_orca_2json

    pid = os.getpid()
    prefix_0 = get_unique_temp_prefix("orca2json", rank=0)
    prefix_1 = get_unique_temp_prefix("orca2json", rank=1)
    prefix_2 = get_unique_temp_prefix("orca2json", rank=2)

    assert prefix_0 == f"orca2json_rank0_pid{pid}_"
    assert prefix_1 == f"orca2json_rank1_pid{pid}_"
    assert prefix_2 == f"orca2json_rank2_pid{pid}_"
    assert prefix_0 != prefix_1 != prefix_2

    mock_shm = tmp_path / "dev_shm"
    mock_shm.mkdir()
    monkeypatch.setattr("lavello_mlips.process_omol25.get_shm_temp_dir", lambda: str(mock_shm))

    created_dir_name = []

    def mock_subprocess_run(cmd, cwd, capture_output, text):
        dir_name = Path(cwd).name
        created_dir_name.append(dir_name)
        payload = {
            "Molecule": {
                "S-Matrix": [[1.0]],
                "dipole": [[[0.0]], [[0.0]], [[0.0]]],
                "quadrupole": [[[0.0]], [[0.0]], [[0.0]], [[0.0]], [[0.0]], [[0.0]]],
            }
        }
        (Path(cwd) / "orca.json").write_text(json.dumps(payload))
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    import struct
    dummy_gbw = bytearray(b"\x00" * 2048)
    struct.pack_into("<q", dummy_gbw, 0, 16)
    atoms = [(1, np.array([0.0, 0.0, 0.0]))]
    shells = [(0, 0, 0, np.array([1.0]), np.array([1.0]))]

    with patch("lavello_mlips.process_omol25._orca_2json_executable", return_value="orca_2json"), \
         patch("subprocess.run", side_effect=mock_subprocess_run):
        _integrals_from_orca_2json(bytes(dummy_gbw), atoms, shells, rank=3)

    assert len(created_dir_name) == 1
    assert created_dir_name[0].startswith(f"orca2json_rank3_pid{pid}_")


def test_get_shm_temp_dir_override_and_env(tmp_path, monkeypatch):
    """Test get_shm_temp_dir with override path and LAVELLO_TMPDIR env var."""
    from lavello_mlips.utils import get_shm_temp_dir

    custom_dir = tmp_path / "custom_temp"
    assert not custom_dir.exists()

    # Override should auto-create directory and return resolved path
    res = get_shm_temp_dir(custom_dir)
    assert custom_dir.is_dir()
    assert res == str(custom_dir.resolve())

    # LAVELLO_TMPDIR env var
    env_dir = tmp_path / "env_temp"
    assert not env_dir.exists()
    monkeypatch.setenv("LAVELLO_TMPDIR", str(env_dir))
    res_env = get_shm_temp_dir()
    assert env_dir.is_dir()
    assert res_env == str(env_dir.resolve())

    # When override is given, it takes precedence over LAVELLO_TMPDIR
    res_override = get_shm_temp_dir(custom_dir)
    assert res_override == str(custom_dir.resolve())


def test_integrals_from_orca_2json_custom_tmp_dir(tmp_path):
    """Test that _integrals_from_orca_2json uses custom temp_dir and cleans up."""
    from lavello_mlips.process_omol25 import _integrals_from_orca_2json
    import struct

    custom_tmp = tmp_path / "my_nvme_scratch"
    created_dirs = []

    def mock_subprocess_run(cmd, cwd, capture_output, text):
        created_dirs.append(Path(cwd))
        assert str(custom_tmp.resolve()) in str(Path(cwd).resolve())
        payload = {
            "Molecule": {
                "S-Matrix": [[1.0]],
                "dipole": [[[0.0]], [[0.0]], [[0.0]]],
                "quadrupole": [[[0.0]], [[0.0]], [[0.0]], [[0.0]], [[0.0]], [[0.0]]],
            }
        }
        (Path(cwd) / "orca.json").write_text(json.dumps(payload))
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    dummy_gbw = bytearray(b"\x00" * 2048)
    struct.pack_into("<q", dummy_gbw, 0, 16)
    atoms = [(1, np.array([0.0, 0.0, 0.0]))]
    shells = [(0, 0, 0, np.array([1.0]), np.array([1.0]))]

    with patch("lavello_mlips.process_omol25._orca_2json_executable", return_value="orca_2json"), \
         patch("subprocess.run", side_effect=mock_subprocess_run):
        overlap, dipole, quadrupole = _integrals_from_orca_2json(
            bytes(dummy_gbw), atoms, shells, temp_dir=custom_tmp
        )

    assert len(created_dirs) == 1
    # Verify created inside custom_tmp and deleted on cleanup
    assert str(custom_tmp.resolve()) in str(created_dirs[0].resolve())
    assert not created_dirs[0].exists()


def test_omol_data_processor_custom_tmp_dir(tmp_path):
    """Test that OmolDataProcessor honors args.tmp_dir."""
    from lavello_mlips.process_omol25 import OmolDataProcessor
    import tempfile

    custom_tmp = tmp_path / "processor_scratch"
    data_source = tmp_path / "omol.json"
    data_source.write_text(json.dumps({"mol/": {"key": "mol.tar.zst"}}))
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()

    class Args:
        def __init__(self):
            self.data_source = data_source
            self.output_dir = output_dir
            self.local_dir = None
            self.login_file = tmp_path / "login.json"
            self.bucket = "mock-bucket"
            self.sample_size = -1
            self.start_index = None
            self.restart = False
            self.memory_threshold_gb = 2.0
            self.batch_size = 10
            self.log_level = "INFO"
            self.log_file = None
            self.tmp_dir = custom_tmp

    (tmp_path / "login.json").write_text('{"access_key": "a", "secret_key": "b"}')
    args = Args()
    processor = OmolDataProcessor(args, rank=0, size=1, comm=None)

    assert processor.tmp_dir == str(custom_tmp.resolve())
    assert tempfile.tempdir == str(custom_tmp.resolve())
    assert custom_tmp.is_dir()

    # Also test default fallback to current directory when tmp_dir is None
    args.tmp_dir = None
    processor_default = OmolDataProcessor(args, rank=0, size=1, comm=None)
    assert processor_default.tmp_dir == str(Path(".").resolve())
    assert tempfile.tempdir == str(Path(".").resolve())


def test_chunked_runs_no_file_collision_and_restart(tmp_path):
    """
    Verify that two concurrent/sequential chunked jobs with disjoint ranges
    (--start-index 0 --sample-size 3 vs --start-index 3 --sample-size 3)
    writing to the same output directory do not overwrite each other's files,
    and that resuming an interrupted chunk with --restart correctly recovers.
    """
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
        def __init__(self, start_idx, sample_sz, restart=False):
            self.data_source = data_source
            self.output_dir = output_dir
            self.local_dir = local_data_dir
            self.login_file = None
            self.bucket = None
            self.start_index = start_idx
            self.sample_size = sample_sz
            self.restart = restart
            self.batch_size = 2
            self.memory_threshold_gb = 10.0
            self.mpi = False
            self.log_file = None

    # --- Job 1: chunk [0, 3] ---
    args1 = Args(0, 3)
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        proc1 = OmolDataProcessor(args1, rank=0, size=1, comm=None)
        assert proc1.group_name == "omol_0_3"
        proc1.run_serial()

    # --- Job 2: chunk [3, 6] ---
    args2 = Args(3, 3)
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        proc2 = OmolDataProcessor(args2, rank=0, size=1, comm=None)
        assert proc2.group_name == "omol_3_6"
        proc2.run_serial()

    # 1. Check final files exist independently without overwriting
    pq1 = output_dir / "props_omol_0_3.parquet"
    xyz1 = output_dir / "structs_omol_0_3.xyz"
    rst1 = data_source.with_name("omol_prefix_0_3_restart.json")

    pq2 = output_dir / "props_omol_3_6.parquet"
    xyz2 = output_dir / "structs_omol_3_6.xyz"
    rst2 = data_source.with_name("omol_prefix_3_6_restart.json")

    assert pq1.exists() and xyz1.exists() and rst1.exists()
    assert pq2.exists() and xyz2.exists() and rst2.exists()

    df1 = pd.read_parquet(pq1)
    df2 = pd.read_parquet(pq2)
    assert set(df1["argonne_rel"]) == {"mol_0/", "mol_1/", "mol_2/"}
    assert set(df2["argonne_rel"]) == {"mol_3/", "mol_4/", "mol_5/"}

    with open(rst1, "r") as f:
        rst1_data = json.load(f)
    with open(rst2, "r") as f:
        rst2_data = json.load(f)

    assert set(rst1_data.keys()) == {"mol_0/", "mol_1/", "mol_2/"}
    assert set(rst2_data.keys()) == {"mol_3/", "mol_4/", "mol_5/"}
    assert all(v["processed"] for v in rst1_data.values())
    assert all(v["processed"] for v in rst2_data.values())

    # --- Test Restart on an interrupted chunk ---
    # Setup another chunk [0, 4] where only 2 items are processed before interruption
    data_source_int = tmp_path / "interrupted_prefix.json"
    int_prefixes = [f"imol_{i}/" for i in range(6)]
    int_data = {p: {"key": "data.tar.zst", "processed": False} for p in int_prefixes}
    with open(data_source_int, "w", encoding="utf-8") as f:
        json.dump(int_data, f)
    create_mock_data(local_data_dir, data_source_int)

    # Initial partial run: simulate processing only first 2 items of [0, 4]
    args_int1 = Args(0, 4)
    args_int1.data_source = data_source_int
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        proc_int1 = OmolDataProcessor(args_int1, rank=0, size=1, comm=None)
        assert proc_int1.group_name == "interrupted_0_4"
        # Process only task 0 and 1
        res0 = proc_int1.process_single(0, None)
        res1 = proc_int1.process_single(1, None)
        proc_int1.flush_recs([res0[0], res1[0]], [res0[1], res1[1]])
        proc_int1.data["imol_0/"]["processed"] = True
        proc_int1.data["imol_1/"]["processed"] = True
        proc_int1._save_restart_file()

    # Verify restart file of interrupted run has 2 processed, 2 unprocessed
    rst_int = data_source_int.with_name("interrupted_prefix_0_4_restart.json")
    assert rst_int.exists()
    with open(rst_int, "r") as f:
        d_saved = json.load(f)
    assert d_saved["imol_0/"]["processed"] is True
    assert d_saved["imol_1/"]["processed"] is True
    assert d_saved["imol_2/"]["processed"] is False
    assert d_saved["imol_3/"]["processed"] is False

    # Now restart the job with --restart --start-index 0 --sample-size 4
    args_int2 = Args(0, 4, restart=True)
    args_int2.data_source = data_source_int
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        proc_int2 = OmolDataProcessor(args_int2, rank=0, size=1, comm=None)
        # Prefixes should only contain the 2 unprocessed items
        assert proc_int2.prefixes == ["imol_2/", "imol_3/"]
        assert proc_int2.indices_to_process == [0, 1]
        proc_int2.run_serial()

    # After restart finishes, verify all 4 items are in final output and restart file
    final_pq_int = output_dir / "props_interrupted_0_4.parquet"
    final_xyz_int = output_dir / "structs_interrupted_0_4.xyz"
    assert final_pq_int.exists()
    assert final_xyz_int.exists()

    df_int = pd.read_parquet(final_pq_int)
    assert len(df_int) == 4
    assert set(df_int["argonne_rel"]) == {"imol_0/", "imol_1/", "imol_2/", "imol_3/"}

    with open(rst_int, "r") as f:
        d_final = json.load(f)
    assert all(v["processed"] for v in d_final.values())


def test_download_chunked_runs_no_file_collision_and_restart(tmp_path, monkeypatch):
    """
    Verify download_omol25 chunked runs with disjoint ranges
    do not collide on restart files and rerunning with --restart works.
    """
    from lavello_mlips.download_omol25 import main as download_main
    dest_dir = tmp_path / "downloads"
    dest_dir.mkdir()
    local_data_dir = tmp_path / "mock_s3_dl"
    data_source = tmp_path / "dl_prefix.json"

    prefixes = [f"dmol_{i}/" for i in range(4)]
    data = {p: {"key": "data.tar.zst", "processed": False} for p in prefixes}
    with open(data_source, "w", encoding="utf-8") as f:
        json.dump(data, f)
    create_mock_data(local_data_dir, data_source)

    monkeypatch.chdir(dest_dir)

    # Job 1: chunk [0, 2]
    test_args1 = [
        "download_omol25",
        "--data-source", str(data_source),
        "--local-dir", str(local_data_dir),
        "--start-index", "0",
        "--sample-size", "2"
    ]
    with patch.object(sys, "argv", test_args1):
        download_main()

    # Job 2: chunk [2, 4]
    test_args2 = [
        "download_omol25",
        "--data-source", str(data_source),
        "--local-dir", str(local_data_dir),
        "--start-index", "2",
        "--sample-size", "2"
    ]
    with patch.object(sys, "argv", test_args2):
        download_main()

    rst1 = data_source.with_name("dl_prefix_0_2_restart.json")
    rst2 = data_source.with_name("dl_prefix_2_4_restart.json")
    assert rst1.exists()
    assert rst2.exists()

    with open(rst1) as f:
        d1 = json.load(f)
    with open(rst2) as f:
        d2 = json.load(f)

    assert set(d1.keys()) == {"dmol_0/", "dmol_1/"}
    assert set(d2.keys()) == {"dmol_2/", "dmol_3/"}
    assert all(v["processed"] for v in d1.values())
    assert all(v["processed"] for v in d2.values())


def test_merge_chunks_unifies_parquets_xyz_and_restart(tmp_path):
    """Test merge_chunks unifies disjoint chunk Parquet, ExtXYZ, and restart JSON files."""
    from lavello_mlips.process_omol25 import merge_chunks
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    data_source = tmp_path / "omol_prefix.json"

    # Base data source
    all_data = {
        f"mol_{i}/": {"key": f"data_{i}.tar.zst", "processed": False} for i in range(4)
    }
    with open(data_source, "w", encoding="utf-8") as f:
        json.dump(all_data, f)

    # Chunk 1: [0, 2]
    df1 = pd.DataFrame([
        {"argonne_rel": "mol_0/", "data_id": 0, "energy": -10.0},
        {"argonne_rel": "mol_1/", "data_id": 1, "energy": -20.0},
    ])
    df1.to_parquet(output_dir / "props_omol_0_2.parquet", index=False)
    at0 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at0.info = {"argonne_rel": "mol_0/", "data_id": 0}
    at1 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.75]])
    at1.info = {"argonne_rel": "mol_1/", "data_id": 1}
    ase.io.write(str(output_dir / "structs_omol_0_2.xyz"), [at0, at1], format="extxyz")

    rst1_data = {
        "mol_0/": {"key": "data_0.tar.zst", "processed": True},
        "mol_1/": {"key": "data_1.tar.zst", "processed": True},
    }
    with open(data_source.with_name("omol_prefix_0_2_restart.json"), "w") as f:
        json.dump(rst1_data, f)

    # Chunk 2: [2, 4]
    df2 = pd.DataFrame([
        {"argonne_rel": "mol_2/", "data_id": 2, "energy": -30.0},
        {"argonne_rel": "mol_3/", "data_id": 3, "energy": -40.0},
    ])
    df2.to_parquet(output_dir / "props_omol_2_4.parquet", index=False)
    at2 = Atoms("O2", positions=[[0, 0, 0], [0, 0, 1.21]])
    at2.info = {"argonne_rel": "mol_2/", "data_id": 2}
    at3 = Atoms("O2", positions=[[0, 0, 0], [0, 0, 1.22]])
    at3.info = {"argonne_rel": "mol_3/", "data_id": 3}
    ase.io.write(str(output_dir / "structs_omol_2_4.xyz"), [at2, at3], format="extxyz")

    rst2_data = {
        "mol_2/": {"key": "data_2.tar.zst", "processed": True},
        "mol_3/": {"key": "data_3.tar.zst", "processed": True},
    }
    with open(data_source.with_name("omol_prefix_2_4_restart.json"), "w") as f:
        json.dump(rst2_data, f)

    # Execute merge_chunks
    final_pq, final_xyz, final_rst = merge_chunks(data_source, output_dir, delete_chunks=True)

    assert final_pq == output_dir / "props_omol.parquet"
    assert final_xyz == output_dir / "structs_omol.xyz"
    assert final_rst == tmp_path / "omol_prefix_restart.json"

    assert final_pq.exists()
    assert final_xyz.exists()
    assert final_rst.exists()

    # Check Parquet content
    df_merged = pd.read_parquet(final_pq)
    assert len(df_merged) == 4
    assert list(df_merged["argonne_rel"]) == ["mol_0/", "mol_1/", "mol_2/", "mol_3/"]

    # Check ExtXYZ content
    atoms_merged = ase.io.read(str(final_xyz), index=":")
    assert len(atoms_merged) == 4
    assert [at.info["argonne_rel"] for at in atoms_merged] == ["mol_0/", "mol_1/", "mol_2/", "mol_3/"]

    # Check consolidated restart JSON
    with open(final_rst) as f:
        d_merged = json.load(f)
    assert len(d_merged) == 4
    assert all(v["processed"] for v in d_merged.values())

    # Check chunk files were deleted since delete_chunks=True
    assert not (output_dir / "props_omol_0_2.parquet").exists()
    assert not (output_dir / "props_omol_2_4.parquet").exists()
    assert not (output_dir / "structs_omol_0_2.xyz").exists()
    assert not (output_dir / "structs_omol_2_4.xyz").exists()
    assert not (tmp_path / "omol_prefix_0_2_restart.json").exists()
    assert not (tmp_path / "omol_prefix_2_4_restart.json").exists()


def test_cli_merge_chunks_flag(tmp_path):
    """Test invoking lavello_mlips with --merge-chunks from CLI."""
    from lavello_mlips.cli import main as cli_main
    output_dir = tmp_path / "cli_outputs"
    output_dir.mkdir()
    data_source = tmp_path / "mydata_prefix.json"

    data = {"m1/": {"key": "k1", "processed": False}}
    with open(data_source, "w") as f:
        json.dump(data, f)

    df = pd.DataFrame([{"argonne_rel": "m1/", "data_id": 1, "energy": -5.0}])
    df.to_parquet(output_dir / "props_mydata_0_1.parquet", index=False)
    at = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at.info = {"argonne_rel": "m1/", "data_id": 1}
    ase.io.write(str(output_dir / "structs_mydata_0_1.xyz"), [at], format="extxyz")

    test_argv = [
        "lavello_mlips",
        "--data-source", str(data_source),
        "--output-dir", str(output_dir),
        "--merge-chunks",
    ]
    with patch.object(sys, "argv", test_argv):
        cli_main()

    assert (output_dir / "props_mydata.parquet").exists()
    assert (output_dir / "structs_mydata.xyz").exists()
    df_res = pd.read_parquet(output_dir / "props_mydata.parquet")
    assert len(df_res) == 1
    assert df_res.iloc[0]["argonne_rel"] == "m1/"


def test_merge_chunks_unifies_rank_parts_without_start_index(tmp_path):
    """
    Test merge_chunks unifies rank part files (e.g. props_rna_0_part_0.parquet,
    props_rna_1_part_0.parquet) generated when runs are performed without --start-index.
    """
    from lavello_mlips.process_omol25 import merge_chunks
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    data_source = tmp_path / "rna_prefix.json"

    data = {
        "rna_0/": {"key": "d0.tar.zst", "processed": False},
        "rna_1/": {"key": "d1.tar.zst", "processed": False},
        "rna_2/": {"key": "d2.tar.zst", "processed": False},
    }
    with open(data_source, "w", encoding="utf-8") as f:
        json.dump(data, f)

    # Rank 0 Part 0: rna_0
    df0 = pd.DataFrame([{"argonne_rel": "rna_0/", "data_id": 0, "energy": -1.0}])
    df0.to_parquet(output_dir / "props_rna_0_part_0.parquet", index=False)
    at0 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at0.info = {"argonne_rel": "rna_0/", "data_id": 0}
    ase.io.write(str(output_dir / "structs_rna_0_part_0.xyz"), [at0], format="extxyz")

    # Rank 1 Part 0: rna_1
    df1 = pd.DataFrame([{"argonne_rel": "rna_1/", "data_id": 1, "energy": -2.0}])
    df1.to_parquet(output_dir / "props_rna_1_part_0.parquet", index=False)
    at1 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.75]])
    at1.info = {"argonne_rel": "rna_1/", "data_id": 1}
    ase.io.write(str(output_dir / "structs_rna_1_part_0.xyz"), [at1], format="extxyz")

    # Rank 1 Part 1: rna_2
    df2 = pd.DataFrame([{"argonne_rel": "rna_2/", "data_id": 2, "energy": -3.0}])
    df2.to_parquet(output_dir / "props_rna_1_part_1.parquet", index=False)
    at2 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.76]])
    at2.info = {"argonne_rel": "rna_2/", "data_id": 2}
    ase.io.write(str(output_dir / "structs_rna_1_part_1.xyz"), [at2], format="extxyz")

    # Execute merge_chunks with delete_chunks=True
    final_pq, final_xyz, final_rst = merge_chunks(data_source, output_dir, delete_chunks=True)

    assert final_pq == output_dir / "props_rna.parquet"
    assert final_xyz == output_dir / "structs_rna.xyz"
    assert final_rst == tmp_path / "rna_prefix_restart.json"

    assert final_pq.exists()
    assert final_xyz.exists()
    assert final_rst.exists()

    df_merged = pd.read_parquet(final_pq)
    assert len(df_merged) == 3
    assert set(df_merged["argonne_rel"]) == {"rna_0/", "rna_1/", "rna_2/"}

    atoms_merged = ase.io.read(str(final_xyz), index=":")
    assert len(atoms_merged) == 3
    assert {at.info["argonne_rel"] for at in atoms_merged} == {"rna_0/", "rna_1/", "rna_2/"}

    with open(final_rst) as f:
        rst_data = json.load(f)
    assert len(rst_data) == 3
    assert all(v["processed"] for v in rst_data.values())

    # Check that part files were deleted
    assert not (output_dir / "props_rna_0_part_0.parquet").exists()
    assert not (output_dir / "props_rna_1_part_0.parquet").exists()
    assert not (output_dir / "props_rna_1_part_1.parquet").exists()
    assert not (output_dir / "structs_rna_0_part_0.xyz").exists()
    assert not (output_dir / "structs_rna_1_part_0.xyz").exists()
    assert not (output_dir / "structs_rna_1_part_1.xyz").exists()


def test_merge_chunks_prunes_mismatched_argonne_rel_and_resets_restart(tmp_path):
    """
    Test that merge_chunks prunes records with mismatched argonne_rel between Parquet and ExtXYZ,
    enforces exact sequence and equality between them, and resets processed=False in restart JSON
    for any mismatched entries.
    """
    from lavello_mlips.process_omol25 import merge_chunks
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    data_source = tmp_path / "rna_prefix.json"

    # Base dataset with 4 structures
    data = {
        "rna_0/": {"key": "d0.tar.zst", "processed": False},
        "rna_1/": {"key": "d1.tar.zst", "processed": False},
        "rna_2/": {"key": "d2.tar.zst", "processed": False},
        "rna_3/": {"key": "d3.tar.zst", "processed": False},
    }
    with open(data_source, "w", encoding="utf-8") as f:
        json.dump(data, f)

    # Chunk restart file where all 4 were optimistically marked processed
    chunk_rst = tmp_path / "rna_prefix_0_4_restart.json"
    chunk_data = {
        "rna_0/": {"key": "d0.tar.zst", "processed": True},
        "rna_1/": {"key": "d1.tar.zst", "processed": True},
        "rna_2/": {"key": "d2.tar.zst", "processed": True},
        "rna_3/": {"key": "d3.tar.zst", "processed": True},
    }
    with open(chunk_rst, "w", encoding="utf-8") as f:
        json.dump(chunk_data, f)

    # Parquet chunk contains rna_0, rna_1, and rna_2 (missing rna_3)
    df = pd.DataFrame([
        {"argonne_rel": "rna_0/", "data_id": 0, "energy": -1.0},
        {"argonne_rel": "rna_1/", "data_id": 1, "energy": -2.0},
        {"argonne_rel": "rna_2/", "data_id": 2, "energy": -3.0},
    ])
    df.to_parquet(output_dir / "props_rna_0_4.parquet", index=False)

    # ExtXYZ chunk contains rna_0, rna_1, and rna_3 (missing rna_2)
    at0 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at0.info = {"argonne_rel": "rna_0/", "data_id": 0}
    at1 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.75]])
    at1.info = {"argonne_rel": "rna_1/", "data_id": 1}
    at3 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.77]])
    at3.info = {"argonne_rel": "rna_3/", "data_id": 3}
    ase.io.write(str(output_dir / "structs_rna_0_4.xyz"), [at0, at1, at3], format="extxyz")

    # Run merge_chunks
    final_pq, final_xyz, final_rst = merge_chunks(data_source, output_dir, delete_chunks=True)

    assert final_pq == output_dir / "props_rna.parquet"
    assert final_xyz == output_dir / "structs_rna.xyz"
    assert final_rst == tmp_path / "rna_prefix_restart.json"

    assert final_pq.exists()
    assert final_xyz.exists()
    assert final_rst.exists()

    # Verify Parquet and ExtXYZ only contain rna_0 and rna_1 (intersection)
    df_merged = pd.read_parquet(final_pq)
    atoms_merged = ase.io.read(str(final_xyz), index=":")

    assert len(df_merged) == 2
    assert len(atoms_merged) == 2
    assert list(df_merged["argonne_rel"]) == ["rna_0/", "rna_1/"]
    assert [at.info["argonne_rel"] for at in atoms_merged] == ["rna_0/", "rna_1/"]

    # Verify restart file marked rna_0/rna_1 as True, and rna_2/rna_3 as False
    with open(final_rst) as f:
        rst_data = json.load(f)

    assert rst_data["rna_0/"]["processed"] is True
    assert rst_data["rna_1/"]["processed"] is True
    assert rst_data["rna_2/"]["processed"] is False
    assert rst_data["rna_3/"]["processed"] is False

    # Intermediate chunks and chunk restart deleted
    assert not (output_dir / "props_rna_0_4.parquet").exists()
    assert not (output_dir / "structs_rna_0_4.xyz").exists()
    assert not chunk_rst.exists()


def test_merge_chunks_drops_file_missing_extxyz_partner_and_continues(tmp_path):
    """
    Test that when an intermediate Parquet part file has no partner ExtXYZ file
    (e.g. structs_scaled_separations_exp_16_part_6.xyz is missing for props_scaled_separations_exp_16_part_6.parquet),
    merge_chunks does NOT raise FileNotFoundError. Instead it drops the unmatched file,
    resets its keys to processed=False in the restart JSON, and merges the rest that match.
    """
    from lavello_mlips.process_omol25 import merge_chunks
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    data_source = tmp_path / "scaled_separations_exp_prefix.json"

    data = {
        "m_good/": {"key": "good.tar.zst", "processed": False},
        "m_orphan/": {"key": "orphan.tar.zst", "processed": False},
    }
    with open(data_source, "w", encoding="utf-8") as f:
        json.dump(data, f)

    # Matching pair: rank 16 part 5
    df_good = pd.DataFrame([{"argonne_rel": "m_good/", "data_id": 1, "energy": -10.0}])
    df_good.to_parquet(output_dir / "props_scaled_separations_exp_16_part_5.parquet", index=False)
    at_good = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at_good.info = {"argonne_rel": "m_good/", "data_id": 1}
    ase.io.write(str(output_dir / "structs_scaled_separations_exp_16_part_5.xyz"), [at_good], format="extxyz")

    # Orphan parquet without matching ExtXYZ: rank 16 part 6
    orphan_pq = output_dir / "props_scaled_separations_exp_16_part_6.parquet"
    df_orphan = pd.DataFrame([{"argonne_rel": "m_orphan/", "data_id": 2, "energy": -20.0}])
    df_orphan.to_parquet(orphan_pq, index=False)
    # Notice: structs_scaled_separations_exp_16_part_6.xyz is intentionally NOT created!

    # Execute merge_chunks
    final_pq, final_xyz, final_rst = merge_chunks(data_source, output_dir, delete_chunks=True)

    assert final_pq == output_dir / "props_scaled_separations_exp.parquet"
    assert final_xyz == output_dir / "structs_scaled_separations_exp.xyz"
    assert final_rst == tmp_path / "scaled_separations_exp_prefix_restart.json"

    assert final_pq.exists()
    assert final_xyz.exists()
    assert final_rst.exists()

    # The orphan file must have been dropped (and unlinked)
    assert not orphan_pq.exists()

    # Merged files must contain only the matching structures (m_good)
    df_merged = pd.read_parquet(final_pq)
    atoms_merged = ase.io.read(str(final_xyz), index=":")
    assert len(df_merged) == 1
    assert len(atoms_merged) == 1
    assert df_merged.iloc[0]["argonne_rel"] == "m_good/"
    assert atoms_merged[0].info["argonne_rel"] == "m_good/"

    # Restart JSON must have m_good/ as True, and m_orphan/ reset to False
    with open(final_rst) as f:
        rst_data = json.load(f)
    assert rst_data["m_good/"]["processed"] is True
    assert rst_data["m_orphan/"]["processed"] is False


def test_merge_chunks_drops_bad_frame_from_extxyz_and_parquet(tmp_path):
    """
    Test that when an ExtXYZ chunk has a corrupted/truncated frame
    (e.g. Frame has 0 atoms, expected 210, raising XYZError),
    merge_chunks does NOT crash. Instead, the corrupted frame is dropped,
    valid frames are recovered, the corrupted frame is pruned from the corresponding Parquet,
    and its processed status in the restart JSON is set to False.
    """
    from lavello_mlips.process_omol25 import merge_chunks
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    data_source = tmp_path / "badframe_prefix.json"

    data = {
        "mol_1/": {"key": "m1.tar.zst", "processed": False},
        "mol_bad/": {"key": "mbad.tar.zst", "processed": False},
        "mol_3/": {"key": "m3.tar.zst", "processed": False},
    }
    with open(data_source, "w", encoding="utf-8") as f:
        json.dump(data, f)

    # Parquet file has records for mol_1, mol_bad, and mol_3
    df = pd.DataFrame([
        {"argonne_rel": "mol_1/", "data_id": 1, "energy": -1.0},
        {"argonne_rel": "mol_bad/", "data_id": 2, "energy": -2.0},
        {"argonne_rel": "mol_3/", "data_id": 3, "energy": -3.0},
    ])
    df.to_parquet(output_dir / "props_badframe_0_3.parquet", index=False)

    # ExtXYZ file has valid mol_1, corrupted mol_bad (0 atoms, expected 210), and valid mol_3
    s_xyz = (
        "2\nProperties=species:S:1:pos:R:3 argonne_rel=mol_1/ data_id=1\nH 0.0 0.0 0.0\nH 0.0 0.0 0.74\n"
        "210\nProperties=species:S:1:pos:R:3 argonne_rel=mol_bad/ data_id=2\n"
        "2\nProperties=species:S:1:pos:R:3 argonne_rel=mol_3/ data_id=3\nO 0.0 0.0 0.0\nO 0.0 0.0 1.2\n"
    )
    xyz_path = output_dir / "structs_badframe_0_3.xyz"
    xyz_path.write_text(s_xyz, encoding="utf-8")

    # Run merge_chunks
    final_pq, final_xyz, final_rst = merge_chunks(data_source, output_dir, delete_chunks=True)

    assert final_pq == output_dir / "props_badframe.parquet"
    assert final_xyz == output_dir / "structs_badframe.xyz"
    assert final_rst == tmp_path / "badframe_prefix_restart.json"

    assert final_pq.exists()
    assert final_xyz.exists()
    assert final_rst.exists()

    # The bad frame must be dropped from BOTH Parquet and ExtXYZ
    df_merged = pd.read_parquet(final_pq)
    atoms_merged = ase.io.read(str(final_xyz), index=":")

    assert len(df_merged) == 2
    assert len(atoms_merged) == 2
    assert list(df_merged["argonne_rel"]) == ["mol_1/", "mol_3/"]
    assert [at.info["argonne_rel"] for at in atoms_merged] == ["mol_1/", "mol_3/"]

    # In restart JSON, mol_1/ and mol_3/ are True, mol_bad/ is False
    with open(final_rst) as f:
        rst_data = json.load(f)
    assert rst_data["mol_1/"]["processed"] is True
    assert rst_data["mol_3/"]["processed"] is True
    assert rst_data["mol_bad/"]["processed"] is False


def test_merge_chunks_without_parquet_adds_prefix_and_warns(tmp_path, caplog):
    """
    Test that when only ExtXYZ chunk files exist (no Parquet files),
    merge_chunks does not abort, prints a warning, and prefixes the merged output with stage1
    (e.g. stage1_structs_rna.xyz).
    """
    from lavello_mlips.process_omol25 import merge_chunks
    import logging

    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    data_source = tmp_path / "rna_prefix.json"

    data = {
        "rna_0/": {"key": "d0.tar.zst", "processed": False},
        "rna_1/": {"key": "d1.tar.zst", "processed": False},
    }
    with open(data_source, "w", encoding="utf-8") as f:
        json.dump(data, f)

    # Only create ExtXYZ parts, NO Parquet parts
    at0 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at0.info = {"argonne_rel": "rna_0/", "data_id": 0}
    ase.io.write(str(output_dir / "structs_rna_0_part_0.xyz"), [at0], format="extxyz")

    at1 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.75]])
    at1.info = {"argonne_rel": "rna_1/", "data_id": 1}
    ase.io.write(str(output_dir / "structs_rna_1_part_0.xyz"), [at1], format="extxyz")

    with caplog.at_level(logging.WARNING):
        final_pq, final_xyz, final_rst = merge_chunks(data_source, output_dir, delete_chunks=True)

    # Verify return values
    assert final_pq is None
    assert final_xyz == output_dir / "stage1_structs_rna.xyz"
    assert final_rst == tmp_path / "rna_prefix_restart.json"

    # Verify warning in logs
    assert any("Merging without Parquet files" in rec.message for rec in caplog.records)
    assert any("stage1" in rec.message for rec in caplog.records)

    # Verify merged file content
    assert final_xyz.exists()
    assert not (output_dir / "props_rna.parquet").exists()
    atoms_merged = ase.io.read(str(final_xyz), index=":")
    assert len(atoms_merged) == 2
    assert {at.info["argonne_rel"] for at in atoms_merged} == {"rna_0/", "rna_1/"}

    # Verify chunks deleted
    assert not (output_dir / "structs_rna_0_part_0.xyz").exists()
    assert not (output_dir / "structs_rna_1_part_0.xyz").exists()

    # Verify restart file updated
    with open(final_rst) as f:
        rst_data = json.load(f)
    assert rst_data["rna_0/"]["processed"] is True
    assert rst_data["rna_1/"]["processed"] is True


def test_merge_chunks_without_parquet_custom_prefix(tmp_path):
    """Test merge_chunks with custom prefix when merging without parquet files."""
    from lavello_mlips.process_omol25 import merge_chunks

    output_dir = tmp_path / "outputs"
    output_dir.mkdir()
    data_source = tmp_path / "custom_prefix.json"

    data = {"c0/": {"key": "c0.tar.zst", "processed": False}}
    with open(data_source, "w", encoding="utf-8") as f:
        json.dump(data, f)

    at = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at.info = {"argonne_rel": "c0/", "data_id": 0}
    ase.io.write(str(output_dir / "structs_custom_0_part_0.xyz"), [at], format="extxyz")

    final_pq, final_xyz, final_rst = merge_chunks(
        data_source, output_dir, delete_chunks=False, prefix="stage2"
    )

    assert final_pq is None
    assert final_xyz == output_dir / "stage2_structs_custom.xyz"
    assert final_xyz.exists()
    atoms_merged = ase.io.read(str(final_xyz), index=":")
    assert len(atoms_merged) == 1
    assert atoms_merged[0].info["argonne_rel"] == "c0/"


def test_cli_merge_chunks_without_parquet(tmp_path):
    """Test invoking lavello_mlips CLI with --merge-chunks and --merge-prefix when no parquet files exist."""
    from lavello_mlips.cli import main as cli_main
    from unittest.mock import patch

    output_dir = tmp_path / "cli_outputs"
    output_dir.mkdir()
    data_source = tmp_path / "mydata_prefix.json"

    data = {"m1/": {"key": "k1", "processed": False}}
    with open(data_source, "w", encoding="utf-8") as f:
        json.dump(data, f)

    at = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    at.info = {"argonne_rel": "m1/", "data_id": 1}
    ase.io.write(str(output_dir / "structs_mydata_0_1.xyz"), [at], format="extxyz")

    test_argv = [
        "lavello_mlips",
        "--data-source", str(data_source),
        "--output-dir", str(output_dir),
        "--merge-chunks",
        "--merge-prefix", "stage1",
    ]
    with patch.object(sys, "argv", test_argv):
        cli_main()

    assert (output_dir / "stage1_structs_mydata.xyz").exists()
    assert not (output_dir / "props_mydata.parquet").exists()
    atoms_res = ase.io.read(str(output_dir / "stage1_structs_mydata.xyz"), index=":")
    assert len(atoms_res) == 1
    assert atoms_res[0].info["argonne_rel"] == "m1/"








