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
    import math
    import pandas as pd
    import ase.io

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
    import re

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
    import math
    import pandas as pd
    import ase.io

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

    import json

    with open(data_source) as f:
        n_total = len(json.load(f))
    half = n_total // 2  # first MPI pass processes this many
    rest = n_total - half  # second MPI pass (restart) picks up the remainder

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
