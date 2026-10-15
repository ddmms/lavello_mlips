import io
import tarfile

import numpy as np
from zstandard import ZstdCompressor

from lavello_mlips.utils import extract_gbw_bytes, extract_gbw_from_path
from lavello_mlips.process_omol25 import (
    AU2B,
    AU2D,
    B2EAA,
    BOHR2A,
    D2EA,
    OmolDataProcessor,
    parse_dipole,
    parse_electric_origin,
    parse_quadrupole,
    shift_multipoles_to_coordinate_origin,
)

QUAD_KEYS = ["xx", "yy", "zz", "xy", "xz", "yz"]


def to_matrix(q6):
    xx, yy, zz, xy, xz, yz = q6
    return np.array([[xx, xy, xz], [xy, yy, yz], [xz, yz, zz]])


def to_six(m):
    return np.array([m[0, 0], m[1, 1], m[2, 2], m[0, 1], m[0, 2], m[1, 2]])


def test_unit_constants_consistent():
    assert np.isclose(AU2D * D2EA, BOHR2A, rtol=1e-6)
    assert np.isclose(AU2B * B2EAA, BOHR2A**2, rtol=1e-6)


def test_parse_dipole_unlabelled_is_au():
    txt = (
        "Total Dipole Moment    :      1.000000000      -2.000000000       0.500000000"
    )
    Dx, Dy, Dz, Mag = parse_dipole(txt)
    assert np.allclose([Dx, Dy, Dz], np.array([1.0, -2.0, 0.5]) * AU2D)
    assert np.isclose(Mag, np.linalg.norm([1.0, -2.0, 0.5]) * AU2D)


def test_parse_electric_origin_bohr_to_angstrom():
    txt = (
        "Position of electric origin ... (  4.1077,  0.0066, -0.0000) \n"
        "Position of magnetic origin ... (  0.0000,  0.0000,  0.0000) \n"
        "Position of electric origin             ...     4.107685     0.006575    -0.000000\n"
    )
    origin = parse_electric_origin(txt)
    assert np.allclose(origin, np.array([4.107685, 0.006575, 0.0]) * BOHR2A)
    assert parse_electric_origin("no origin here") is None


def test_shift_matches_point_charges():
    rng = np.random.default_rng(0)
    q = np.array([1.3, -0.4, 0.8, -0.7])
    r = rng.normal(size=(4, 3)) * 2.0
    R = np.array([0.7, -1.2, 2.5])
    d = r - R
    mu_R = q @ d
    quad_R = np.einsum("i,ia,ib->ab", q, d, d)

    mu0, quad0 = shift_multipoles_to_coordinate_origin(mu_R, to_six(quad_R), q.sum(), R)

    assert np.allclose(mu0, q @ r)
    assert np.allclose(to_matrix(quad0), np.einsum("i,ia,ib->ab", q, r, r))


def test_shift_neutral_dipole_invariant():
    mu = np.array([0.3, -0.1, 0.2])
    mu0, quad0 = shift_multipoles_to_coordinate_origin(
        mu, None, 0.0, np.array([5.0, -3.0, 1.0])
    )
    assert np.allclose(mu0, mu)
    assert quad0 is None


def make_orca_buffer(mu_au, quad_au, origin_bohr, charge, include_origin=True):
    quad_b = np.asarray(quad_au) * AU2B
    origin_line = (
        "Position of electric origin             ...     "
        + "     ".join(f"{v:.6f}" for v in origin_bohr)
        if include_origin
        else ""
    )
    orca_out = f"""
Number of atoms .............................. 2
-----------------------
CARTESIAN COORDINATES (ANGSTROEM)
-----------------------
  He     1.000000    0.500000   -0.200000
  H      1.000000    0.500000    0.574000

{origin_line}

Total Dipole Moment    :      {mu_au[0]:.9f}       {mu_au[1]:.9f}       {mu_au[2]:.9f}

                XX              YY              ZZ              XY              XZ              YZ
  TOT      {"   ".join(f"{v:.9f}" for v in quad_au)} (a.u.)
           {"   ".join(f"{v:.9f}" for v in quad_b)} (Buckingham)

NO  OCC          E(Eh)            E(eV)
 0  2.0000      -1.5000         -40.8171
 1  0.0000       0.2000           5.4423
****ORCA TERMINATED NORMALLY****
"""
    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
        for name, content in [
            ("orca.out", orca_out),
            ("orca.inp", f"* xyz {charge} 1"),
            ("orca.property.txt", ""),
        ]:
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content.encode("utf-8")))
    return io.BytesIO(ZstdCompressor().compress(tar_buffer.getvalue()))


def test_process_buffer_shifts_multipoles_and_keeps_orca_values():
    mu_au = np.array([-0.55, 0.0024, 0.1])
    quad_au = np.array([-14.41, -25.26, -25.28, 0.32, 0.05, -0.02])
    origin_bohr = np.array([4.107685, 0.006575, -0.3])
    charge = 1

    buf = make_orca_buffer(mu_au, quad_au, origin_bohr, charge)
    rec, atoms = OmolDataProcessor._process_buffer(None, buf, "x")

    assert rec["total_charge_e"] == charge
    assert rec["status_dipole"] == "OK"
    assert rec["status_quadrupole"] == "OK"
    assert "dipole" not in atoms.calc.results

    R = origin_bohr * BOHR2A
    assert np.allclose([rec[f"R_orca_origin_{c}"] for c in "xyz"], R)

    mu_D_orca = np.array([rec[f"dipole_{c}_D_orca"] for c in "xyz"])
    quad_B_orca = np.array([rec[f"Q_{k}_B_orca"] for k in QUAD_KEYS])
    assert np.allclose(mu_D_orca, mu_au * AU2D)
    assert np.allclose(quad_B_orca, quad_au * AU2B, atol=1e-8)
    assert np.isclose(rec["Q_isotropic_B_orca"], quad_B_orca[:3].mean())

    mu = mu_au * AU2D * D2EA
    quad = to_matrix(quad_au * AU2B * B2EAA)
    mu0 = mu + charge * R
    quad0 = quad + np.outer(R, mu) + np.outer(mu, R) + charge * np.outer(R, R)

    assert np.allclose([rec[f"dipole_{c}_eA"] for c in "xyz"], mu0)
    assert np.allclose([rec[f"dipole_{c}_D"] for c in "xyz"], mu0 / D2EA)
    assert np.isclose(rec["dipole_mag_D"], np.linalg.norm(mu0) / D2EA)
    assert np.allclose([rec[f"Q_{k}_eAA"] for k in QUAD_KEYS], to_six(quad0))
    assert np.allclose([rec[f"Q_{k}_B"] for k in QUAD_KEYS], to_six(quad0) / B2EAA)
    assert np.isclose(rec["Q_isotropic_B"], np.trace(quad0) / 3.0 / B2EAA)


def test_process_buffer_without_origin_keeps_orca_values_only():
    quad_au = np.array([-10.0, -11.0, -12.0, 0.5, 0.0, 0.0])
    buf = make_orca_buffer(
        np.array([0.1, 0.2, 0.3]), quad_au, np.zeros(3), 0, include_origin=False
    )

    rec, _ = OmolDataProcessor._process_buffer(None, buf, "x")

    assert rec["status_dipole"] == "NO_ORIGIN"
    assert rec["status_quadrupole"] == "NO_ORIGIN"
    assert "dipole_x_D" not in rec
    assert "Q_xx_B" not in rec
    assert np.isclose(rec["dipole_x_D_orca"], 0.1 * AU2D)
    assert np.isclose(rec["Q_xx_B_orca"], quad_au[0] * AU2B)
    assert parse_quadrupole("") is None


def test_extract_gbw_bytes():
    c = ZstdCompressor()
    gbw_content = b"TEST_GBW_DATA"
    zstd_data = c.compress(gbw_content)
    assert extract_gbw_bytes(zstd_data) == gbw_content

    # In a tar archive compressed with zstd
    tar_buf = io.BytesIO()
    with tarfile.open(fileobj=tar_buf, mode="w") as t:
        ti = tarfile.TarInfo("orca.gbw")
        ti.size = len(gbw_content)
        t.addfile(ti, io.BytesIO(gbw_content))
    zstd_tar = c.compress(tar_buf.getvalue())
    assert extract_gbw_bytes(zstd_tar) == gbw_content

    # Uncompressed fallback
    assert extract_gbw_bytes(gbw_content) == gbw_content


def test_extract_gbw_from_path(tmp_path):
    c = ZstdCompressor()
    gbw_content = b"TEST_GBW_FROM_PATH"
    folder = tmp_path / "calc1"
    folder.mkdir()
    zstd_path = folder / "orca.gbw.zstd0"
    zstd_path.write_bytes(c.compress(gbw_content))

    gbw_path = folder / "orca.gbw"
    assert not gbw_path.exists()

    res = extract_gbw_from_path(gbw_path)
    assert res == gbw_content
    assert gbw_path.exists()
    assert gbw_path.read_bytes() == gbw_content


def test_process_buffer_with_orca_gbw_zstd0():
    from zstandard import ZstdDecompressor
    c = ZstdCompressor()
    gbw_content = b"GBW_INSIDE_TAR"
    zstd_gbw = c.compress(gbw_content)

    quad_au = np.array([-10.0, -11.0, -12.0, 0.5, 0.0, 0.0])
    buf = make_orca_buffer(
        np.array([0.1, 0.2, 0.3]), quad_au, np.zeros(3), 0, include_origin=True
    )
    decomp = io.BytesIO()
    ZstdDecompressor().copy_stream(buf, decomp)
    decomp.seek(0)

    new_tar_buf = io.BytesIO()
    with tarfile.open(fileobj=decomp, mode="r") as old_tar:
        with tarfile.open(fileobj=new_tar_buf, mode="w") as new_tar:
            for m in old_tar.getmembers():
                f = old_tar.extractfile(m)
                new_tar.addfile(m, f)
            ti = tarfile.TarInfo("orca.gbw.zstd0")
            ti.size = len(zstd_gbw)
            new_tar.addfile(ti, io.BytesIO(zstd_gbw))

    new_buf = io.BytesIO(c.compress(new_tar_buf.getvalue()))
    rec, _ = OmolDataProcessor._process_buffer(None, new_buf, "x")
    assert rec["status_dipole"] == "OK"

