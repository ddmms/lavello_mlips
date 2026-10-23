import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import time
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from ase.io import read, write
from mpi4py import MPI
from tarfile import open as tar_open
from tqdm import tqdm
from zstandard import ZstdDecompressor

import ase.parallel
from .s3_processor import S3DataProcessor
from .utils import (
    json_load,
    json_dump,
    extract_gbw_bytes,
    extract_gbw_from_path,
    get_shm_temp_dir,
    get_unique_temp_prefix,
)

_shm_temp = get_shm_temp_dir()
if _shm_temp:
    tempfile.tempdir = _shm_temp

ase.parallel.world = ase.parallel.DummyMPI()

# ---------- constants ----------
AU2D = 2.541746  # a.u. → Debye
D2EA = 0.20819434  # Debye → e·Å
AU2B = 1.34503431  # a.u. → Buckingham
B2EAA = 0.208194333  # Buckingham → e·Å²
BOHR2A = 0.529177210903  # bohr → Å

MiB = 1024**2
GiB = 1024 * MiB

# Tags for MPI communication
TAG_DONE = 2
TAG_RESULT = 5

logger = logging.getLogger(__name__)
logging.captureWarnings(True)


def safe_read_extxyz(path: Union[str, Path]) -> List[Any]:
    """
    Safely reads an ExtXYZ file into a list of Atoms objects.
    If a frame is corrupted or truncated (e.g. XYZError: Frame has 0 atoms, expected 210),
    gracefully recovers all valid frames from the file, discarding corrupted frames.
    """
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return []

    # Fast path: try standard ASE read
    try:
        atoms = read(str(path), index=":", format="extxyz")
        if not isinstance(atoms, list):
            atoms = [atoms] if atoms else []
        valid_atoms = [at for at in atoms if len(at) > 0]
        if len(valid_atoms) == len(atoms):
            return valid_atoms
    except Exception as e_fast:
        logger.warning(
            f"Standard ExtXYZ read failed for {path.name} ({e_fast}). Attempting robust frame-by-frame recovery..."
        )

    # Robust fallback: line-by-line frame recovery
    frames = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        i = 0
        n_lines = len(lines)
        header_re = re.compile(r"^\s*(\d+)\s*$")
        dropped_count = 0

        while i < n_lines:
            m = header_re.match(lines[i])
            if not m:
                i += 1
                continue

            natoms = int(m.group(1))
            if natoms <= 0:
                i += 1
                continue

            if i + 1 + natoms >= n_lines:
                # Truncated frame (e.g. at EOF)
                dropped_count += 1
                i += 1
                continue

            chunk_lines = lines[i : i + 2 + natoms]
            chunk_str = "".join(chunk_lines)
            try:
                at = read(StringIO(chunk_str), format="extxyz")
                if len(at) == natoms:
                    frames.append(at)
                    i += 2 + natoms
                else:
                    dropped_count += 1
                    i += 1
            except Exception:
                dropped_count += 1
                i += 1

        if dropped_count > 0:
            logger.warning(
                f"ExtXYZ file {path.name}: recovered {len(frames)} valid frames, dropped {dropped_count} bad/truncated frame(s)."
            )
    except Exception as e_fallback:
        logger.error(f"Fallback ExtXYZ recovery failed for {path.name}: {e_fallback}", exc_info=True)

    return frames


_RE_ARGONNE_REL = re.compile(r'argonne_rel=(?:"([^"]+)"|(\S+))')
_RE_HEADER = re.compile(r"^\s*(\d+)\s*$")


def fast_extract_keys_from_extxyz_file(in_path: Union[str, Path]) -> set:
    """Quickly extract all argonne_rel keys from valid frames in an ExtXYZ file without building Atoms objects."""
    in_path = Path(in_path)
    if not in_path.exists() or in_path.stat().st_size == 0:
        return set()
    keys = set()
    with open(in_path, "r", encoding="utf-8", errors="replace", buffering=1024 * 1024) as f_in:
        pending_line = None
        while True:
            line1 = pending_line if pending_line is not None else f_in.readline()
            pending_line = None
            if not line1:
                break
            stripped = line1.strip()
            if not stripped:
                continue
            try:
                natoms = int(stripped)
            except ValueError:
                continue
            if natoms <= 0:
                continue
            line2 = f_in.readline()
            if not line2:
                break
            if _RE_HEADER.match(line2):
                pending_line = line2
                continue

            m = _RE_ARGONNE_REL.search(line2)
            rel = (m.group(1) or m.group(2)) if m else None

            is_truncated = False
            atom_count = 0
            for _ in range(natoms):
                l = f_in.readline()
                if not l:
                    is_truncated = True
                    break
                if _RE_HEADER.match(l):
                    is_truncated = True
                    pending_line = l
                    break
                atom_count += 1

            if is_truncated or atom_count < natoms:
                continue

            if rel:
                keys.add(rel)
    return keys


def fast_stream_extxyz_file(
    in_path: Union[str, Path],
    f_out: Any,
    seen_keys: set,
    saved_keys: set,
    skip_keys: Optional[set] = None,
    dropped_keys: Optional[set] = None,
) -> Tuple[int, int]:
    """
    Stream frames directly from an ExtXYZ file into an open output file without parsing
    into ase.Atoms objects.

    - Validates natoms and frame boundaries.
    - Discards corrupted/truncated frames (e.g., unexpected EOF or missing atom lines).
    - Extracts `argonne_rel` to deduplicate frames.
    - Tracks saved keys in `saved_keys` and dropped keys in `dropped_keys`.
    - Returns (valid_count, dropped_count).
    """
    in_path = Path(in_path)
    if not in_path.exists() or in_path.stat().st_size == 0:
        return 0, 0

    valid_count = 0
    dropped_count = 0

    with open(in_path, "r", encoding="utf-8", errors="replace", buffering=1024 * 1024) as f_in:
        pending_line = None
        while True:
            line1 = pending_line if pending_line is not None else f_in.readline()
            pending_line = None
            if not line1:
                break
            stripped = line1.strip()
            if not stripped:
                continue
            try:
                natoms = int(stripped)
            except ValueError:
                dropped_count += 1
                continue
            if natoms <= 0:
                dropped_count += 1
                continue

            line2 = f_in.readline()
            if not line2:
                dropped_count += 1
                break
            if _RE_HEADER.match(line2):
                dropped_count += 1
                pending_line = line2
                continue

            m = _RE_ARGONNE_REL.search(line2)
            rel = (m.group(1) or m.group(2)) if m else None

            atom_lines = []
            is_truncated = False
            for _ in range(natoms):
                l = f_in.readline()
                if not l:
                    is_truncated = True
                    break
                if _RE_HEADER.match(l):
                    is_truncated = True
                    pending_line = l
                    break
                atom_lines.append(l)

            if is_truncated or len(atom_lines) < natoms:
                dropped_count += 1
                if rel and dropped_keys is not None:
                    dropped_keys.add(rel)
                continue

            if skip_keys and rel and rel in skip_keys:
                continue
            if rel and rel in seen_keys:
                continue
            if rel:
                seen_keys.add(rel)
                saved_keys.add(rel)

            if not line1.endswith("\n"):
                line1 += "\n"
            if not line2.endswith("\n"):
                line2 += "\n"
            f_out.write(line1)
            f_out.write(line2)
            f_out.writelines(atom_lines)
            if atom_lines and not atom_lines[-1].endswith("\n"):
                f_out.write("\n")
            valid_count += 1

    if dropped_count > 0:
        logger.warning(
            f"ExtXYZ stream {in_path.name}: streamed {valid_count} valid frames, dropped {dropped_count} bad/truncated frame(s)."
        )

    return valid_count, dropped_count


# ---------- dipole ----------
RE_DIP = re.compile(
    r"Total\s+Dipole\s+Moment(?:\s*\((a\.u\.|Debye)\))?\s*:\s*"
    r"([+-]?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?)\s+"
    r"([+-]?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?)\s+"
    r"([+-]?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?)",
    re.I,
)


# ---------- quadrupole ----------
RE_QUAD = re.compile(
    r"TOT\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+\(a\.u\.\)\n"
    r"\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+\(Buckingham\)",
    re.DOTALL,
)


def parse_quadrupole(txt: str) -> Optional[Tuple[float, float, float, float, float, float, float]]:
    if not txt:
        return None
    match = RE_QUAD.search(txt)

    if match:
        vals = [float(g) for g in match.groups()]
        # Buckingham values are the last 6 groups
        xx, yy, zz, xy, xz, yz = vals[6:12]
        q_iso = (xx + yy + zz) / 3.0
        return (xx, yy, zz, xy, xz, yz, q_iso)
    return None


def parse_dipole(txt: str) -> Optional[Tuple[float, float, float, float]]:
    if not txt:
        return None
    best = None
    for m in RE_DIP.finditer(txt):
        unit = (m.group(1) or "").lower()
        x, y, z = map(float, m.group(2, 3, 4))
        # ORCA prints the unlabelled "Total Dipole Moment" components in a.u.
        if unit in ("", "a.u."):
            x, y, z = x * AU2D, y * AU2D, z * AU2D
        best = (x, y, z)
        if unit == "debye":
            break  # prefer explicit Debye value
    if not best:
        return None
    Dx, Dy, Dz = best
    Mag = (Dx * Dx + Dy * Dy + Dz * Dz) ** 0.5
    return Dx, Dy, Dz, Mag


# ---------- electric origin ----------
RE_ORIGIN = re.compile(
    r"Position\s+of\s+electric\s+origin\s*\.\.\.\s*\(?\s*"
    r"([+-]?\d+(?:\.\d+)?)\s*,?\s*([+-]?\d+(?:\.\d+)?)\s*,?\s*([+-]?\d+(?:\.\d+)?)",
    re.I,
)

# ORCA quadrupole component order: XX, YY, ZZ, XY, XZ, YZ
QUAD_ROWS = np.array([0, 1, 2, 0, 0, 1])
QUAD_COLS = np.array([0, 1, 2, 1, 2, 2])


def parse_electric_origin(txt: str) -> Optional[np.ndarray]:
    """Origin of ORCA's electric multipoles in Å (ORCA prints it in bohr)."""
    matches = RE_ORIGIN.findall(txt or "")
    if not matches:
        return None
    return np.array(matches[-1], dtype=float) * BOHR2A


def shift_multipoles_to_coordinate_origin(
    mu: np.ndarray, quad6: Optional[np.ndarray], charge: float, origin: np.ndarray
) -> Tuple[np.ndarray, Optional[np.ndarray]]:
    """Move a dipole (3,) and raw Cartesian quadrupole (6,) from `origin` to (0, 0, 0).

    Inputs must share one unit system (e.g. e·Å, e·Å², Å). With R = origin:
    mu0 = mu + q R,  Q0 = Q + R mu^T + mu R^T + q R R^T.
    """
    R = np.asarray(origin, dtype=float)
    mu = np.asarray(mu, dtype=float)
    mu0 = mu + charge * R
    if quad6 is None:
        return mu0, None
    theta = np.zeros((3, 3))
    theta[QUAD_ROWS, QUAD_COLS] = quad6
    theta[QUAD_COLS, QUAD_ROWS] = quad6
    theta0 = theta + np.outer(R, mu) + np.outer(mu, R) + charge * np.outer(R, R)
    return mu0, theta0[QUAD_ROWS, QUAD_COLS]


# ---------- spin dipole / spin quadrupole from ORCA GBW + density ----------
# ORCA 6.0 GBW shell record. Contraction coefficients sit at +304; the AO
# index where the next shell starts sits at +604. The last shell of an atom
# instead stores (angular momentum, AO index) in the last 8 bytes of the
# previous record.
_GBW_SHELL_STRIDE = 608
_GBW_PATH_BYTES = 1024
# Control header that ORCA writes in front of the geometry in *.SHARKINP.tmp.
# One-electron integrals only depend on the geometry and shells that follow.
_SHARK_HEADER = (
    " 1.0000000000000000e-10 9.9999999999999994e-12 -1.0000000000000000e+00 0 3 1.0000000000000001e-15 9.9999999999999995e-07 1.1000000000000001e+00",
    "0 0 1 1 1 0 0 0 0 0 1 20 0 9.0000000000000000e+00 ",
    "10 6 6 44 0 150 3 20 0 0 1.0000000000000000e+00 1.0000000000000000e-10",
    "10 12 6 44 0 300 3 20 0 0 1.0000000000000000e+00 1.0000000000000000e-10",
    "12 6 6 44 0 300 3 20 0 0 1.0000000000000000e+00 1.0000000000000000e-10",
    "10 6 6 44 0 150 3 20 0 0 1.0000000000000000e+00 1.0000000000000000e-10",
    " 0 0.0000000000000000e+00 1.0000000000000000e+00 0.0000000000000000e+00 0 0.0000000000000000e+00 1.0000000000000000e+00 0.0000000000000000e+00",
    " -1 -1.0000000000000000e+00 ",
    "-1 -1 ",
)
# orca_2json quadrupole matrices: xx, yy, zz, xy, xz, yz.
_QUAD_PAIRS = ((0, 0), (1, 1), (2, 2), (0, 1), (0, 2), (1, 2))


def _gbw_pointers(data: bytes) -> Tuple[int, int]:
    geometry, basis = struct.unpack_from("<qq", data, 8)
    return geometry, basis


def _gbw_geometry(data: bytes, offset: int) -> List[Tuple[int, np.ndarray]]:
    natom = struct.unpack_from("<i", data, offset)[0]
    pos = offset + 4
    atoms = []
    for _ in range(natom):
        xyz = np.array(struct.unpack_from("<3d", data, pos), dtype=np.float64)
        atomic_number = struct.unpack_from("<i", data, pos + 48)[0]
        atoms.append((atomic_number, xyz))
        pos += 68
    return atoms


def _gbw_shells(data: bytes, basis_offset: int, natom: int):
    """AO shells in ORCA storage order.

    Each entry is (atom index, angular momentum, AO index, exponents, coefficients).
    """
    pos = basis_offset + 24
    shells = []
    atom = 0
    ao = 0
    stride = _GBW_SHELL_STRIDE
    while atom < natom and pos + stride <= len(data):
        nprim = struct.unpack_from("<i", data, pos)[0]
        if nprim == 0:
            pos += 8
            atom += 1
            continue
        if not 1 <= nprim <= 40:
            break
        exponents = np.array(
            [struct.unpack_from("<d", data, pos + 8 + 8 * k)[0] for k in range(nprim)],
            dtype=np.float64,
        )
        if np.any(exponents <= 0.0):
            break
        ao_end = struct.unpack_from("<i", data, pos + 604)[0]
        if ao_end <= ao:
            angular, ao_tag = struct.unpack_from("<ii", data, pos - 8)
            if ao_tag != ao or not 0 <= angular <= 6:
                break
            ao_end = ao + 2 * angular + 1
        nfn = ao_end - ao
        if nfn < 1 or nfn % 2 == 0 or nfn > 15:
            break
        coefficients = np.array(
            [struct.unpack_from("<d", data, pos + 304 + 8 * k)[0] for k in range(nprim)],
            dtype=np.float64,
        )
        shells.append((atom, (nfn - 1) // 2, ao, exponents, coefficients))
        ao = ao_end
        pos += stride
    if not shells:
        raise RuntimeError("could not read AO shells from the GBW")
    return shells


def _orca_2json_executable() -> str:
    configured = os.environ.get("ORCA_2JSON")
    if configured:
        return configured
    found = shutil.which("orca_2json")
    if not found:
        raise RuntimeError("orca_2json not found; set ORCA_2JSON to the ORCA 6.0 binary")
    return found


def _sharkinp_text(atoms, shells) -> str:
    per_atom: List[List] = [[] for _ in atoms]
    for atom, angular, ao, exponents, coefficients in shells:
        per_atom[atom].append((angular, ao, exponents, coefficients))
    nao = shells[-1][2] + 2 * shells[-1][1] + 1
    lines = [f"   {len(atoms)} {nao}  {len(shells)}   1   0   2   2  -1      2000   0   1   1   1   1   1"]
    lines.extend(_SHARK_HEADER)
    for index, (atomic_number, xyz) in enumerate(atoms):
        lines.append(
            f"  {atomic_number:2d}  {len(per_atom[index]):2d}  -1   {float(atomic_number):.16e}"
            f"  {xyz[0]:.16e}  {xyz[1]:.16e}  {xyz[2]:.16e}  0.0"
        )
    for group in per_atom:
        for angular, ao, exponents, coefficients in group:
            lines.append(f"   {angular}   {len(exponents)}  {ao}")
            for exponent, coefficient in zip(exponents, coefficients):
                lines.append(f"     {exponent:.16e}   {coefficient:.16e}")
    lines.extend([" 0 0 ", " 0 0 ", " 0 0 ", "[ov]"])
    return "\n".join(lines) + "\n"


def _retarget_gbw_basename(gbw: bytes, basename: str) -> bytes:
    """Point the GBW's embedded calculation path at ``basename`` (no extension)."""
    path_offset = struct.unpack_from("<q", gbw, 0)[0]
    token = basename.encode() + b"\x00"
    if path_offset <= 0 or path_offset + _GBW_PATH_BYTES > len(gbw) or len(token) > _GBW_PATH_BYTES:
        raise RuntimeError("GBW does not contain an ORCA 6.0 calculation-path field")
    rewritten = bytearray(gbw)
    rewritten[path_offset : path_offset + _GBW_PATH_BYTES] = b"\x00" * _GBW_PATH_BYTES
    rewritten[path_offset : path_offset + len(token)] = token
    return bytes(rewritten)


def _integrals_from_orca_2json(
    gbw: bytes,
    atoms,
    shells,
    rank: Optional[int] = None,
    temp_dir: Optional[Union[str, Path]] = None,
):
    """Overlap, dipole, and quadrupole integrals in the GBW's own AO basis."""
    executable = _orca_2json_executable()
    if temp_dir is not None:
        temp_parent = get_shm_temp_dir(temp_dir)
    else:
        temp_parent = get_shm_temp_dir()
    temp_prefix = get_unique_temp_prefix("orca2json", rank=rank)
    directory = tempfile.mkdtemp(prefix=temp_prefix, dir=temp_parent)
    try:
        base = str(Path(directory) / "orca")
        Path(base + ".gbw").write_bytes(_retarget_gbw_basename(gbw, base))
        Path(base + ".SHARKINP.tmp").write_text(_sharkinp_text(atoms, shells))
        Path(base + ".json.conf").write_text(
            json.dumps(
                {
                    "1elIntegrals": ["S"],
                    "1elPropertyIntegrals": ["dipole", "quadrupole"],
                }
            )
        )
        try:
            import msgspec
            use_msgpack = True
        except ImportError:
            use_msgpack = False

        cmd = [executable, base + ".gbw"]
        if use_msgpack:
            cmd.append("-msgpack")

        completed = subprocess.run(
            cmd,
            cwd=directory,
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "").strip()
            raise RuntimeError(f"orca_2json failed: {detail[-2000:]}")
        if completed.stderr and completed.stderr.strip():
            logger.warning("orca_2json warning: %s", completed.stderr.strip())

        msgpack_path = Path(base + ".msgpack")
        json_path = Path(base + ".json")
        if msgpack_path.exists():
            payload = msgspec.msgpack.decode(msgpack_path.read_bytes())
        elif json_path.exists():
            try:
                import orjson
                payload = orjson.loads(json_path.read_bytes())
            except ImportError:
                payload = json.loads(json_path.read_text())
        else:
            raise FileNotFoundError(f"Neither {msgpack_path.name} nor {json_path.name} was generated by orca_2json.")
    finally:
        shutil.rmtree(directory, ignore_errors=True)

    molecule = payload["Molecule"]
    overlap = np.asarray(molecule["S-Matrix"], dtype=np.float64)
    dipole = np.asarray(molecule["dipole"], dtype=np.float64)
    quadrupole = np.asarray(molecule["quadrupole"], dtype=np.float64)
    nao = overlap.shape[0]
    if overlap.shape != (nao, nao) or dipole.shape != (3, nao, nao) or quadrupole.shape != (6, nao, nao):
        raise RuntimeError(
            f"unexpected integral shapes S{overlap.shape} dipole{dipole.shape} quadrupole{quadrupole.shape}"
        )
    return overlap, dipole, quadrupole


def _unpack_triangular(values: np.ndarray, nao: int) -> np.ndarray:
    if values.size != nao * (nao + 1) // 2:
        raise RuntimeError(
            f"packed density length {values.size} is not {nao}*({nao}+1)/2"
        )
    matrix = np.zeros((nao, nao), dtype=np.float64)
    r_idx, c_idx = np.triu_indices(nao)
    matrix[r_idx, c_idx] = values
    matrix[c_idx, r_idx] = values
    return matrix


def spin_multipoles_from_orca(
    gbw: bytes,
    density_npz: bytes,
    rank: Optional[int] = None,
    temp_dir: Optional[Union[str, Path]] = None,
) -> Dict[str, Any]:
    """Spin dipole and traceless spin quadrupole at the coordinate origin.

    Overlap and multipole integrals come from ``orca_2json`` in the GBW AO
    basis. The origin is the laboratory frame of the GBW geometry, (0, 0, 0),
    not the centre of mass. ``spin_dipole_eA`` is in e·Å and
    ``spin_quadrupole_traceless`` is the traceless Cartesian tensor in e·Å²,
    raw second moment minus one third of its trace.
    """
    with np.load(BytesIO(density_npz)) as packed:
        if "orca.scfr" not in packed.files:
            return {
                "spin_dipole_eA": [0.0, 0.0, 0.0],
                "spin_quadrupole_traceless": [
                    [0.0, 0.0, 0.0],
                    [0.0, 0.0, 0.0],
                    [0.0, 0.0, 0.0],
                ],
                "spin_electrons": 0.0,
                "status_spin_multipole": "OK",
                "spin_multipole_origin": "coordinate_origin",
            }
        scfr_packed = np.asarray(packed["orca.scfr"])
        if not np.any(scfr_packed):
            return {
                "spin_dipole_eA": [0.0, 0.0, 0.0],
                "spin_quadrupole_traceless": [
                    [0.0, 0.0, 0.0],
                    [0.0, 0.0, 0.0],
                    [0.0, 0.0, 0.0],
                ],
                "spin_electrons": 0.0,
                "status_spin_multipole": "OK",
                "spin_multipole_origin": "coordinate_origin",
            }

    geometry_offset, basis_offset = _gbw_pointers(gbw)
    atoms = _gbw_geometry(gbw, geometry_offset)
    shells = _gbw_shells(gbw, basis_offset, len(atoms))
    overlap, dipole, quadrupole6 = _integrals_from_orca_2json(
        gbw, atoms, shells, rank=rank, temp_dir=temp_dir
    )
    nao = overlap.shape[0]

    spin = _unpack_triangular(scfr_packed, nao)

    n_spin = float(np.einsum("ij,ji->", spin, overlap))
    mu_au = np.einsum("ij,aij->a", spin, dipole)
    second = np.einsum("ij,aij->a", spin, quadrupole6)
    raw_au = np.zeros((3, 3), dtype=np.float64)
    for value, (row, col) in zip(second, _QUAD_PAIRS):
        raw_au[row, col] = raw_au[col, row] = value
    traceless_au = raw_au - np.eye(3) * (np.trace(raw_au) / 3.0)
    mu = mu_au * BOHR2A
    quad = traceless_au * (BOHR2A ** 2)
    return {
        "spin_dipole_eA": [float(x) for x in mu],
        "spin_quadrupole_traceless": [[float(x) for x in row] for row in quad],
        "spin_electrons": n_spin,
        "status_spin_multipole": "OK",
        "spin_multipole_origin": "coordinate_origin",
    }


def _attach_spin_multipoles(
    rec: Dict[str, Any],
    gbw: Optional[bytes],
    density_npz: Optional[bytes],
    structure_id: Optional[str] = None,
    rank: Optional[int] = None,
    temp_dir: Optional[Union[str, Path]] = None,
) -> None:
    if not gbw or not density_npz:
        rec["status_spin_multipole"] = "MISSING"
        return
    ident = structure_id or rec.get("argonne_rel") or rec.get("data_id") or "unknown"
    if rec.get("total_charge_e") is None:
        logger.warning("Spin multipole failed for %s: missing total charge", ident)
        rec["status_spin_multipole"] = "NO_CHARGE"
        return
    try:
        rec.update(
            spin_multipoles_from_orca(
                gbw, density_npz, rank=rank, temp_dir=temp_dir
            )
        )
    except Exception as exc:
        logger.error("Spin multipole failed for %s: %s", ident, exc, exc_info=True)
        rec["status_spin_multipole"] = "ERROR"



# ---------- charge/multiplicity ----------
RE_CHARGE_MULT = re.compile(
    r"(?:Total\s+Charge|Overall\s+charge\s+of\s+the\s+system)\s*[:=]\s*(-?\d+)|"
    r"Multiplicity\s*[:=]\s*(\d+)", re.I)
RE_XYZ = re.compile(r"^\s*\*\s*xyz(?:file)?\s+(-?\d+)\s+(\d+)\b.*$", flags=re.I | re.M)

def parse_charge_mult(txt: str) -> Tuple[Optional[int], Optional[int]]:
    Q = None
    M = None
    for m in RE_CHARGE_MULT.finditer(txt):
        q_match = m.group(1)
        if q_match is not None:
            try:
                Q = int(q_match)
            except ValueError:
                # Ignore unparsable charge value; leave Q as-is (None or previous match).
                logger.warning("Failed to parse charge value from match %r in text; ignoring.", q_match)
        else:
            m_match = m.group(2)
            if m_match is not None:
                try:
                    M = int(m_match)
                except ValueError:
                    # Ignore unparsable multiplicity value; leave M as-is (None or previous match).
                    logger.warning("Failed to parse multiplicity value from match %r in text; ignoring.", m_match)

    m = RE_XYZ.search(txt)
    if m:
        try:
            Q = int(m.group(1))
            M = int(m.group(2))
        except ValueError:
            # Ignore unparsable XYZ header values; leave Q/M as determined above.
            logger.warning(
                "Failed to parse charge/multiplicity from XYZ header match %r; ignoring.", m.groups()
            )
    return Q, M


def cog(coords):
    return [sum(v[i] for v in coords) / (len(coords)) for i in range(3)]


def cnc(Z, coords):
    Zsum = sum(Z)
    return [
        sum(Z[k] * coords[k][i] for k in range(len(coords))) / Zsum for i in range(3)
    ]


def geom_sha1(elems, coords, ndp: int = 6) -> Optional[str]:
    h = hashlib.sha1()
    for e, (x, y, z) in zip(elems, coords):
        h.update(
            f"{e}:{round(x, ndp):.6f}:{round(y, ndp):.6f}:{round(z, ndp):.6f};".encode()
        )
    return h.hexdigest()


# ---------- eigenvalues ----------
RE_COLS = re.compile(r"\bNO\b.*\bOCC\b.*E\(Eh\).*E\(eV\)", re.I)
RE_FLOAT = r"[+-]?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?"
RE_ROW = re.compile(rf"^\s*\d+\s+({RE_FLOAT})\s+({RE_FLOAT})\s+({RE_FLOAT})\s*$")
RE_ROW_T = re.compile(rf"^\s*\d+\s+({RE_FLOAT})\s+({RE_FLOAT})\s+({RE_FLOAT})\s+\S.*$")


def homo_lumo(evals, occs, thr=1e-3):
    if not evals:
        return None, None
    occ_idx = [i for i, o in enumerate(occs) if o is not None and o > thr]
    virt_idx = [i for i, o in enumerate(occs) if o is not None and o <= thr]
    if not occ_idx:
        return None, (evals[virt_idx[0]] if virt_idx else None)
    h = max(occ_idx)
    virt_above = [i for i in virt_idx if i > h] or virt_idx
    l_idx = min(virt_above) if virt_above else None
    return evals[h], (evals[l_idx] if l_idx is not None else None)


def parse_eigens(txt: str) -> Optional[Dict[str, Any]]:
    if not txt:
        return None
    lines = txt.splitlines()
    blocks = []
    i = 0
    while i < len(lines):
        if RE_COLS.search(lines[i]):
            tag = "R"
            back = (
                (lines[i - 1].upper() if i >= 1 else "")
                + " "
                + (lines[i - 2].upper() if i >= 2 else "")
            )
            # ORCA labels unrestricted blocks "SPIN UP" / "SPIN DOWN".
            # "SPIN DOWN" must be tested first; it does not contain "SPIN UP".
            if "SPIN DOWN" in back or "BETA" in back:
                tag = "B"
            elif "SPIN UP" in back or "ALPHA" in back:
                tag = "A"
            i += 1
            rows = []
            while i < len(lines):
                row = lines[i]
                if not row.strip() or not any(ch.isdigit() for ch in row):
                    break
                m = RE_ROW.match(row) or RE_ROW_T.match(row)
                if not m:
                    break
                occ = float(m.group(1))
                e_ev = float(m.group(3))
                rows.append((occ, e_ev))
                i += 1
            if rows:
                blocks.append((tag, rows))
                continue
        i += 1
    if not blocks:
        return None
    A = B = R = []
    for tag, rows in blocks:
        if tag == "A":
            A = rows
        elif tag == "B":
            B = rows
        else:
            R = rows
    res: Dict[str, Any] = {}
    if A or B:
        res["spin_case"] = "UKS"
        a_occ, a_eV = zip(*A) if A else ([], [])
        b_occ, b_eV = zip(*B) if B else ([], [])
        a_occ, a_eV = list(a_occ), list(a_eV)
        b_occ, b_eV = list(b_occ), list(b_eV)
        ha, la = homo_lumo(a_eV, a_occ)
        hb, lb = homo_lumo(b_eV, b_occ)
        res.update(
            {
                "alpha_homo_eV": ha,
                "alpha_lumo_eV": la,
                "beta_homo_eV": hb,
                "beta_lumo_eV": lb,
                "n_orb_a": len(a_eV),
                "n_orb_b": len(b_eV),
            }
        )
    elif R:
        r_occ, r_eV = zip(*R) if R else ([], [])
        r_occ, r_eV = list(r_occ), list(r_eV)
        ha, la = homo_lumo(r_eV, r_occ)
        res.update(
            {
                "spin_case": "RKS",
                "alpha_homo_eV": ha,
                "alpha_lumo_eV": la,
                "beta_homo_eV": ha,
                "beta_lumo_eV": la,
                "n_orb_a": len(r_eV),
                "n_orb_b": 0,
            }
        )
    return res


def build_orca_record(
    txt_out: str,
    txt_prop: str,
    txt_inp: str,
    gbw: Optional[bytes],
    density: Optional[bytes],
    structure_id: Optional[str] = None,
    rank: Optional[int] = None,
    temp_dir: Optional[Union[str, Path]] = None,
) -> Tuple[Dict[str, Any], Any]:
    """Parse one ORCA calculation, including spin multipoles when a GBW and density exist."""
    rec: Dict[str, Any] = {}
    if structure_id:
        rec["argonne_rel"] = structure_id
        rec["data_id"] = str(structure_id).strip("/")
    Q, M = parse_charge_mult((txt_out or "") + "\n" + (txt_inp or ""))
    rec["total_charge_e"] = Q
    rec["multiplicity"] = M

    dip = parse_dipole(((txt_prop or "") + "\n" + (txt_out or "")))
    quad = parse_quadrupole(txt_out or "")
    origin = parse_electric_origin(txt_out or "")
    if dip:
        rec.update(
            {
                "dipole_x_D_orca": dip[0],
                "dipole_y_D_orca": dip[1],
                "dipole_z_D_orca": dip[2],
                "dipole_mag_D_orca": dip[3],
            }
        )
    if quad:
        rec.update(
            {
                "Q_xx_B_orca": quad[0],
                "Q_yy_B_orca": quad[1],
                "Q_zz_B_orca": quad[2],
                "Q_xy_B_orca": quad[3],
                "Q_xz_B_orca": quad[4],
                "Q_yz_B_orca": quad[5],
                "Q_isotropic_B_orca": quad[6],
            }
        )
    mu_eA = quad_eAA = None
    if dip and origin is not None and Q is not None:
        rec.update(
            {
                "R_orca_origin_x": float(origin[0]),
                "R_orca_origin_y": float(origin[1]),
                "R_orca_origin_z": float(origin[2]),
            }
        )
        mu_eA, quad_eAA = shift_multipoles_to_coordinate_origin(
            np.array(dip[:3]) * D2EA,
            np.array(quad[:6]) * B2EAA if quad else None,
            Q,
            origin,
        )

    if mu_eA is not None:
        Dx, Dy, Dz = (mu_eA / D2EA).tolist()
        Mag = (Dx * Dx + Dy * Dy + Dz * Dz) ** 0.5
        rec.update(
            {
                "dipole_x_D": Dx,
                "dipole_y_D": Dy,
                "dipole_z_D": Dz,
                "dipole_mag_D": Mag,
                "dipole_x_eA": Dx * D2EA,
                "dipole_y_eA": Dy * D2EA,
                "dipole_z_eA": Dz * D2EA,
                "status_dipole": "OK",
            }
        )
    else:
        rec["status_dipole"] = "MISSING" if not dip else "NO_ORIGIN"

    if quad_eAA is not None:
        xx, yy, zz, xy, xz, yz = (quad_eAA / B2EAA).tolist()
        q = (xx + yy + zz) / 3.0
        rec.update(
            {
                "Q_xx_B": xx,
                "Q_yy_B": yy,
                "Q_zz_B": zz,
                "Q_xy_B": xy,
                "Q_xz_B": xz,
                "Q_yz_B": yz,
                "Q_isotropic_B": q,
                "Q_xx_eAA": xx * B2EAA,
                "Q_yy_eAA": yy * B2EAA,
                "Q_zz_eAA": zz * B2EAA,
                "Q_xy_eAA": xy * B2EAA,
                "Q_xz_eAA": xz * B2EAA,
                "Q_yz_eAA": yz * B2EAA,
                "Q_isotropic_eAA": q * B2EAA,
                "status_quadrupole": "OK",
            }
        )
        raw = np.zeros((3, 3))
        raw[QUAD_ROWS, QUAD_COLS] = quad_eAA
        raw[QUAD_COLS, QUAD_ROWS] = quad_eAA
        traceless = raw - np.eye(3) * (np.trace(raw) / 3.0)
        rec["quadrupole_eA_traceless"] = traceless.tolist()
    else:
        rec["status_quadrupole"] = "MISSING" if not quad else "NO_ORIGIN"

    atoms = read(StringIO(txt_out), format="orca-output")
    # ASE only shifts ORCA's dipole to (0, 0, 0) for pre-6 output; drop it to avoid ambiguity
    if atoms.calc is not None:
        atoms.calc.results.pop("dipole", None)
    rec["natoms"] = len(atoms)
    coords = atoms.positions.tolist()
    elems = list(atoms.symbols)
    Zs = list(atoms.numbers)
    rec["geom_sha1"] = geom_sha1(elems, coords)
    R_cog = cog(coords)
    R_com = atoms.get_center_of_mass().tolist()
    R_cnc = cnc(Zs, coords)
    rec.update({"R_cog_x": R_cog[0], "R_cog_y": R_cog[1], "R_cog_z": R_cog[2]})
    rec.update({"R_com_x": R_com[0], "R_com_y": R_com[1], "R_com_z": R_com[2]})
    rec.update({"R_cnc_x": R_cnc[0], "R_cnc_y": R_cnc[1], "R_cnc_z": R_cnc[2]})

    eig = parse_eigens(txt_out or "")
    if eig:
        rec["status_eigs"] = "OK"
        for k in [
            "spin_case",
            "alpha_homo_eV",
            "alpha_lumo_eV",
            "beta_homo_eV",
            "beta_lumo_eV",
            "n_orb_a",
            "n_orb_b",
        ]:
            if k in eig:
                rec[k] = eig[k]
    else:
        rec["status_eigs"] = "MISSING"

    _attach_spin_multipoles(
        rec, gbw, density, structure_id=structure_id, rank=rank, temp_dir=temp_dir
    )

    for k, v in rec.items():
        if v is not None:
            atoms.info[k] = v
    for key in ("spin_dipole_eA", "spin_quadrupole_traceless", "quadrupole_eA_traceless"):
        if rec.get(key) is not None:
            atoms.info[key] = np.asarray(rec[key], dtype=float)
    return rec, atoms


class OmolDataProcessor(S3DataProcessor):
    """
    Derived processor for Omol data, handling MPI orchestration and Orca parsing.
    """

    def __init__(self, args: argparse.Namespace, rank: int, size: int, comm: Any) -> None:
        super().__init__(args.login_file, args.bucket, args.local_dir)
        self.args = args
        self.rank = rank
        self.size = size
        self.comm = comm
        self.win = None
        self.output_dir = args.output_dir
        self.restart = args.restart
        self.sample_size = args.sample_size
        self.memory_threshold = args.memory_threshold_gb * GiB
        self.chunk_idx = 0
        self.tmp_dir = get_shm_temp_dir(getattr(args, "tmp_dir", None) or Path("."))
        if self.tmp_dir:
            tempfile.tempdir = self.tmp_dir
        self.stop_requested = False
        self.is_slurm = "SLURM_JOB_ID" in os.environ

        def handle_signal(signum, frame):
            logger.warning(
                f"Rank {self.rank} received signal {signum}. Will exit gracefully."
            )
            self.stop_requested = True

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        if hasattr(signal, "SIGUSR1"):
            signal.signal(signal.SIGUSR1, handle_signal)

        self.files_to_process = Path(args.data_source)
        is_restart_file = self.files_to_process.name.endswith("_restart.json")
        if is_restart_file:
            self.restart = True

        if self.rank == 0:
            logger.info(f"Initializing dataset from: {self.files_to_process.resolve()}")

        with open(self.files_to_process, "r", encoding="utf-8") as f:
            full_data = json_load(f)

        all_keys = list(full_data.keys())
        total_keys = len(all_keys)

        stem = self.files_to_process.stem
        stem_no_restart = stem[:-8] if stem.endswith("_restart") else stem
        m_chunk = re.search(r"^(.*?)(?:_(\d+)_(\d+))$", stem_no_restart)
        start_idx = getattr(args, "start_index", None)
        sample_size = getattr(args, "sample_size", -1)
        if sample_size is None:
            sample_size = -1

        if m_chunk:
            stem_base = m_chunk.group(1)
            chunk_start = int(m_chunk.group(2))
            chunk_end = int(m_chunk.group(3))
            is_chunk = True
            chunk_tag = f"_{chunk_start}_{chunk_end}"
            base_group = stem_base.replace("_prefix", "")
            self.restart_file = self.files_to_process
            assigned_keys = all_keys
        elif start_idx is not None and (start_idx > 0 or (0 < sample_size < total_keys)):
            stem_base = stem_no_restart
            base_group = stem_base.replace("_prefix", "")
            if sample_size < 0:
                end_idx = total_keys
            else:
                end_idx = min(start_idx + sample_size, total_keys)
            is_chunk = True
            chunk_tag = f"_{start_idx}_{end_idx}"
            self.restart_file = self.files_to_process.with_name(
                f"{stem_base}{chunk_tag}_restart.json"
            )
            if self.restart and self.restart_file.exists():
                try:
                    with open(self.restart_file, "r", encoding="utf-8") as f_rst:
                        saved_chunk_data = json_load(f_rst)
                    for k, v in saved_chunk_data.items():
                        if k in full_data and isinstance(v, dict):
                            full_data[k].update(v)
                except Exception as e_rst:
                    logger.warning(f"Could not read restart file {self.restart_file}: {e_rst}")
            assigned_keys = all_keys[start_idx : end_idx]
        else:
            stem_base = stem_no_restart
            base_group = stem_base.replace("_prefix", "")
            is_chunk = False
            chunk_tag = ""
            if is_restart_file:
                self.restart_file = self.files_to_process
            else:
                self.restart_file = self.files_to_process.with_name(
                    f"{stem_base}_restart.json"
                )
            if self.restart and not is_restart_file and self.restart_file.exists():
                try:
                    with open(self.restart_file, "r", encoding="utf-8") as f_rst:
                        saved_rst = json_load(f_rst)
                    for k, v in saved_rst.items():
                        if k in full_data and isinstance(v, dict):
                            full_data[k].update(v)
                except Exception as e_rst:
                    logger.warning(f"Could not read restart file {self.restart_file}: {e_rst}")
            assigned_keys = all_keys

        self.is_chunk = is_chunk

        self.group_name = f"{base_group}{chunk_tag}"
        self.merge_prefix = getattr(args, "merge_prefix", "stage1") or "stage1"
        self.output_props_final = self.output_dir / f"props_{self.group_name}.parquet"
        self.output_xyz_final = self.output_dir / f"structs_{self.group_name}.xyz"
        self.output_props_rank = (
            self.output_dir / f"props_{self.group_name}_{self.rank}.parquet"
        )

        log_file = getattr(args, "log_file", None)
        if log_file:
            self.logfile = Path(log_file)
        elif hasattr(self, "output_dir") and self.output_dir:
            self.logfile = Path(self.output_dir) / f"{self.group_name}.log"
        else:
            self.logfile = Path(f"{self.group_name}.log")
        self._setup_logging()

        self.data = {k: full_data[k] for k in assigned_keys}

        part_pattern = re.compile(
            rf"^props_{re.escape(self.group_name)}_(\d+)_part_(\d+)\.parquet$"
        )
        xyz_pattern = re.compile(
            rf"^structs_{re.escape(self.group_name)}_(\d+)_part_(\d+)\.xyz$"
        )

        if self.restart:
            if self.rank == 0:
                logger.info(
                    "Scanning output directory to recover already processed files..."
                )
                recovered_count = 0
                parquet_files = []
                if self.output_props_final.exists():
                    parquet_files.append(self.output_props_final)
                for pf in sorted(self.output_dir.glob(f"props_{self.group_name}_*.parquet")):
                    if part_pattern.match(pf.name) and not pf.name.endswith(".tmp"):
                        parquet_files.append(pf)

                all_disk_keys = set()
                had_repair = False

                for pf in tqdm(parquet_files, desc="Recovering parts", disable=len(parquet_files) == 0):
                    xyz_file = pf.parent / pf.name.replace(
                        "props_", "structs_"
                    ).replace(".parquet", ".xyz")

                    if not xyz_file.exists():
                        logger.warning(
                            f"Discarding incomplete part {pf.name}: Missing ExtXYZ partner {xyz_file.name}"
                        )
                        pf.unlink(missing_ok=True)
                        had_repair = True
                        continue

                    try:
                        df_rel = pd.read_parquet(pf, columns=["argonne_rel"])
                    except Exception as e_pq:
                        logger.warning(f"Discarding corrupt Parquet {pf.name}: {e_pq}")
                        pf.unlink(missing_ok=True)
                        xyz_file.unlink(missing_ok=True)
                        had_repair = True
                        continue

                    try:
                        atoms_list = safe_read_extxyz(xyz_file)
                    except Exception as e_xyz:
                        logger.warning(f"Discarding corrupt ExtXYZ {xyz_file.name}: {e_xyz}")
                        pf.unlink(missing_ok=True)
                        xyz_file.unlink(missing_ok=True)
                        had_repair = True
                        continue

                    pq_keys = (
                        set(df_rel["argonne_rel"].dropna().unique())
                        if "argonne_rel" in df_rel.columns
                        else set()
                    )
                    xyz_keys = {
                        at.info.get("argonne_rel")
                        for at in atoms_list
                        if at.info.get("argonne_rel") is not None
                    }
                    common_keys = pq_keys & xyz_keys

                    if not common_keys:
                        logger.warning(
                            f"Discarding {pf.name} and {xyz_file.name}: zero overlapping structures ({len(pq_keys)} Parquet keys vs {len(xyz_keys)} XYZ keys)."
                        )
                        pf.unlink(missing_ok=True)
                        xyz_file.unlink(missing_ok=True)
                        had_repair = True
                        continue

                    mismatch = (
                        len(df_rel) != len(atoms_list)
                        or len(df_rel) != len(common_keys)
                        or len(atoms_list) != len(common_keys)
                        or pq_keys != xyz_keys
                    )

                    if mismatch:
                        had_repair = True
                        mismatched_keys = pq_keys ^ xyz_keys
                        logger.warning(
                            f"Length/key mismatch in {pf.name} and {xyz_file.name}: "
                            f"{len(df_rel)} Parquet rows vs {len(atoms_list)} XYZ structures. "
                            f"Recovering {len(common_keys)} intersecting structures. "
                            f"{len(mismatched_keys)} mismatched keys will be reprocessed."
                        )
                        try:
                            df_full = pd.read_parquet(pf)
                            df_pruned = df_full[df_full["argonne_rel"].isin(common_keys)].drop_duplicates(
                                subset=["argonne_rel"], keep="last"
                            )
                            seen_xyz = set()
                            atoms_pruned = []
                            for at in reversed(atoms_list):
                                rel = at.info.get("argonne_rel")
                                if rel in common_keys and rel not in seen_xyz:
                                    seen_xyz.add(rel)
                                    atoms_pruned.append(at)
                            atoms_pruned.reverse()

                            tmp_pq = pf.with_suffix(".parquet.tmp")
                            tmp_xyz = xyz_file.with_suffix(".xyz.tmp")
                            df_pruned.to_parquet(tmp_pq, index=False)
                            write(str(tmp_xyz), atoms_pruned, format="extxyz")
                            tmp_pq.replace(pf)
                            tmp_xyz.replace(xyz_file)
                        except Exception as e_repair:
                            logger.error(
                                f"Failed to repair/prune {pf.name}: {e_repair}. Discarding.",
                                exc_info=True,
                            )
                            pf.unlink(missing_ok=True)
                            xyz_file.unlink(missing_ok=True)
                            continue

                    for x in common_keys:
                        all_disk_keys.add(x)
                        if x in self.data and not self.data[x].get("processed", False):
                            self.data[x]["processed"] = True
                            recovered_count += 1

                if parquet_files:
                    for x, item in self.data.items():
                        if item.get("processed", False) and x not in all_disk_keys:
                            logger.warning(
                                f"Key '{x}' marked processed in restart file but not found in disk outputs. Resetting to processed=False."
                            )
                            self.data[x]["processed"] = False
                            had_repair = True

                if had_repair or recovered_count > 0:
                    self._save_restart_file()

                if recovered_count > 0 or all_disk_keys:
                    logger.info(
                        f"Recovered {len(all_disk_keys)} total valid items from disk ({recovered_count} newly marked processed)."
                    )

            if self.size > 1 and self.comm is not None:
                self.data = self.comm.bcast(self.data, root=0)

        self.prefixes = [
            x for x in self.data if not self.data[x].get("processed", False)
        ]

        if self.rank == 0:
            logger.info(
                f"{len(self.prefixes)} files to process out of {len(self.data)}"
            )
            if not self.restart:
                self.output_props_final.unlink(missing_ok=True)
                self.output_xyz_final.unlink(missing_ok=True)
                for pf in self.output_dir.glob(f"props_{self.group_name}_*.parquet"):
                    if part_pattern.match(pf.name):
                        pf.unlink(missing_ok=True)
                for xf in self.output_dir.glob(f"structs_{self.group_name}_*.xyz"):
                    if xyz_pattern.match(xf.name):
                        xf.unlink(missing_ok=True)
            self.output_dir.mkdir(parents=True, exist_ok=True)

        if self.restart and self.output_dir.exists():
            rank_part_pattern = re.compile(
                rf"^(?:props|structs)_{re.escape(self.group_name)}_{self.rank}_part_(\d+)\.(?:parquet|xyz)$"
            )
            existing_indices = [
                int(m.group(1))
                for f in self.output_dir.iterdir()
                if f.is_file() and (m := rank_part_pattern.match(f.name))
            ]
            self.chunk_idx = max(existing_indices) + 1 if existing_indices else 0
        else:
            self.chunk_idx = 0

        if getattr(self, "is_chunk", False):
            self.indices_to_process = list(range(len(self.prefixes)))
        else:
            sample_size = getattr(self.args, "sample_size", -1)
            if sample_size is not None and sample_size > 0:
                self.indices_to_process = list(range(min(sample_size, len(self.prefixes))))
            else:
                self.indices_to_process = list(range(len(self.prefixes)))
        num_tasks = len(self.indices_to_process)

        if getattr(self.args, "batch_size", None) is not None:
            self.batch_size = max(1, self.args.batch_size)
        else:
            active_workers = (
                max(1, self.size - 1) if self.size > 1 else max(1, self.size)
            )
            tasks_per_worker = max(1, num_tasks / active_workers)
            self.batch_size = max(100, int(tasks_per_worker * 0.01))

        if self.rank == 0:
            logger.info(f"Using flush batch size of {self.batch_size}")

    def _setup_logging(self) -> None:
        """Configures logging for this processor rank so all errors and warnings are recorded in the log file."""
        root_logger = logging.getLogger()
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(log_format)

        log_level_str = getattr(self.args, "log_level", "INFO")
        log_level = getattr(logging, str(log_level_str).upper(), logging.INFO)
        if root_logger.level == logging.NOTSET or root_logger.level > log_level:
            root_logger.setLevel(log_level)

        target_path = Path(self.logfile).resolve()

        has_file_handler = False
        for h in root_logger.handlers[:]:
            if isinstance(h, logging.FileHandler):
                try:
                    if Path(h.baseFilename).resolve() == target_path:
                        has_file_handler = True
                    else:
                        root_logger.removeHandler(h)
                        h.close()
                except Exception:
                    pass

        if not has_file_handler:
            self.logfile.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(self.logfile, mode="a", encoding="utf-8")
            fh.setFormatter(formatter)
            fh.setLevel(log_level)
            root_logger.addHandler(fh)

        has_stream_handler = any(
            isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
            for h in root_logger.handlers
        )
        if not has_stream_handler:
            sh = logging.StreamHandler(sys.stderr)
            sh.setFormatter(formatter)
            sh.setLevel(logging.WARNING if self.rank > 0 else log_level)
            root_logger.addHandler(sh)

        logging.captureWarnings(True)
        warnings_logger = logging.getLogger("py.warnings")
        warnings_logger.propagate = True

        prev_excepthook = sys.excepthook

        def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                if prev_excepthook:
                    prev_excepthook(exc_type, exc_value, exc_traceback)
                else:
                    sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            logger.critical(
                f"Rank {self.rank} uncaught exception: {exc_value}",
                exc_info=(exc_type, exc_value, exc_traceback),
            )
            if prev_excepthook and prev_excepthook is not handle_uncaught_exception:
                prev_excepthook(exc_type, exc_value, exc_traceback)

        sys.excepthook = handle_uncaught_exception

    def flush_recs(self, recs: List[Dict[str, Any]], all_atoms: Optional[List[Any]] = None) -> None:
        """Flush a batch of records to Parquet, and optionally atoms to ExtXYZ."""
        if not recs:
            return
        df = pd.DataFrame(recs)
        path = (
            self.output_dir
            / f"props_{self.group_name}_{self.rank}_part_{self.chunk_idx}.parquet"
        )
        df.to_parquet(path, index=False)
        if all_atoms:
            xyz_path = (
                self.output_dir
                / f"structs_{self.group_name}_{self.rank}_part_{self.chunk_idx}.xyz"
            )
            write(str(xyz_path), all_atoms, format="extxyz")
        self.chunk_idx += 1

    def _process_buffer(
        self,
        buffer: BytesIO,
        x: str,
        gbw_extra: Optional[bytes] = None,
        density_extra: Optional[bytes] = None,
    ) -> Optional[Tuple[Dict[str, Any], Any]]:
        """Parse a .tar.zst buffer; returns (rec dict, ASE Atoms) or None."""
        try:
            decompressor = ZstdDecompressor()
            decompressed_buffer = BytesIO()
            decompressor.copy_stream(buffer, decompressed_buffer)
            decompressed_buffer.seek(0)

            with tar_open(fileobj=decompressed_buffer, mode="r") as tar:
                names = set(tar.getnames())
                txt_out = tar.extractfile("orca.out").read().decode("utf-8")
                txt_prop = tar.extractfile("orca.property.txt").read().decode("utf-8")
                txt_inp = tar.extractfile("orca.inp").read().decode("utf-8")

                gbw = gbw_extra
                if gbw is None:
                    if "orca.gbw" in names:
                        gbw = tar.extractfile("orca.gbw").read()
                    elif "orca.gbw.zstd0" in names:
                        gbw = extract_gbw_bytes(tar.extractfile("orca.gbw.zstd0").read())
                    else:
                        for m in tar.getmembers():
                            m_base = Path(m.name).name
                            if m_base == "orca.gbw":
                                f = tar.extractfile(m)
                                if f:
                                    gbw = f.read()
                                    break
                            elif m_base in ("orca.gbw.zstd0", "orca.gbw.zst", "orca.gbw.zstd"):
                                f = tar.extractfile(m)
                                if f:
                                    gbw = extract_gbw_bytes(f.read())
                                    break

                density = density_extra
                if density is None:
                    if "density_mat.npz" in names:
                        density = tar.extractfile("density_mat.npz").read()
                    else:
                        for m in tar.getmembers():
                            if Path(m.name).name == "density_mat.npz":
                                f = tar.extractfile(m)
                                if f:
                                    density = f.read()
                                    break

            return build_orca_record(
                txt_out,
                txt_prop,
                txt_inp,
                gbw,
                density,
                structure_id=x,
                rank=getattr(self, "rank", None),
                temp_dir=getattr(self, "tmp_dir", None),
            )
        except Exception as e:
            logger.error(f"Error parsing buffer for {x}: {e}", exc_info=True)
            return None

    def process_single(self, idx: int, s3_client: Any = None) -> Optional[Tuple[Dict[str, Any], Any, str]]:
        """Processes a single task synchronously. Returns (rec, atoms, x) or None."""
        start_time = time.time()
        x = self.prefixes[idx]
        raw_key = self.data[x]["key"]
        key_list = [raw_key] if isinstance(raw_key, str) else list(raw_key)

        tar_key = next((k for k in key_list if ".tar" in k), key_list[0])
        source = x + tar_key
        rec: Dict[str, Any] = {}
        try:
            rec["argonne_rel"] = x
            rec["data_id"] = source.split("/")[-1]

            buffer = BytesIO()
            if self.local_dir:
                local_path = self.local_dir / source
                if not local_path.exists():
                    logger.warning(f"Local file '{local_path}' not found. Skipping.")
                    if x in self.data:
                        self.data[x]["processed"] = False
                    return None
                content = local_path.read_bytes()
                buffer.write(content)
            else:
                try:
                    response = s3_client.get_object(Bucket=self.args.bucket, Key=source)
                    buffer.write(response["Body"].read())
                except Exception as e_s3:
                    logger.error(f"S3 get_object error for {source}: {e_s3}")
                    if x in self.data:
                        self.data[x]["processed"] = False
                    return None

            gbw_extra = None
            density_extra = None
            transfer_failed = False

            # Check if orca.gbw/orca.gbw.zstd0 or density_mat.npz are present as separate keys
            for k in key_list:
                if k == tar_key:
                    continue
                k_name = Path(k).name
                if "orca.gbw" in k_name:
                    try:
                        if self.local_dir:
                            gbw_extra = extract_gbw_from_path(self.local_dir / x / "orca.gbw")
                            if gbw_extra is None:
                                p = self.local_dir / (x + k)
                                if p.exists():
                                    gbw_extra = extract_gbw_bytes(p.read_bytes())
                                else:
                                    logger.warning(f"Local GBW file '{p}' not found for {x}.")
                                    transfer_failed = True
                        elif s3_client:
                            resp = s3_client.get_object(Bucket=self.args.bucket, Key=x + k)
                            gbw_extra = extract_gbw_bytes(resp["Body"].read())
                    except Exception as e_gbw:
                        logger.warning(f"Failed to fetch/extract GBW for {x}: {e_gbw}")
                        transfer_failed = True
                elif "density_mat.npz" in k_name:
                    try:
                        if self.local_dir:
                            p = self.local_dir / (x + k)
                            if not p.exists():
                                p = self.local_dir / x / "density_mat.npz"
                            if p.exists():
                                density_extra = p.read_bytes()
                            else:
                                logger.warning(f"Local density_mat file '{p}' not found for {x}.")
                                transfer_failed = True
                        elif s3_client:
                            resp = s3_client.get_object(Bucket=self.args.bucket, Key=x + k)
                            density_extra = resp["Body"].read()
                    except Exception as e_dens:
                        logger.warning(f"Failed to fetch density_mat for {x}: {e_dens}")
                        transfer_failed = True

            if transfer_failed:
                logger.error(f"Transfer failure for one or more files in {x}. Marking processed=False.")
                if x in self.data:
                    self.data[x]["processed"] = False
                return None

            # If local_dir was provided, also check if orca.gbw exists on disk even if not in key_list
            if self.local_dir and gbw_extra is None:
                gbw_extra = extract_gbw_from_path(self.local_dir / x / "orca.gbw")
            if self.local_dir and density_extra is None:
                dens_p = self.local_dir / x / "density_mat.npz"
                if dens_p.is_file():
                    density_extra = dens_p.read_bytes()

            buffer.seek(0)
            result = self._process_buffer(buffer, x, gbw_extra=gbw_extra, density_extra=density_extra)
            if result is None:
                if x in self.data:
                    self.data[x]["processed"] = False
                return None
            parsed_rec, atoms = result
            rec.update(parsed_rec)

            rec["process_time_s"] = time.time() - start_time
            # Update atoms.info with the final rec (includes argonne_rel, data_id, process_time_s)
            atoms.info["argonne_rel"] = x
            atoms.info["data_id"] = rec["data_id"]
            return rec, atoms, x
        except Exception as e:
            logger.error(f"Error processing {source}: {e}", exc_info=True)
            if x in self.data:
                self.data[x]["processed"] = False
            return None


    def _manager_loop(self) -> None:
        """Rank 0 Result Collector loop (Hybrid RMA)."""
        start_time = time.time()
        num_tasks = len(self.indices_to_process)
        logger.info(f"Manager starting. Collecting results for {num_tasks} tasks.")

        processed_count = 0
        pbar = tqdm(total=num_tasks, desc="Total Progress")

        # We wait for results until all workers have finished
        active_workers = self.size - 1

        while active_workers > 0:
            status = MPI.Status()
            # We use ANY_TAG because workers might send TAG_RESULT or TAG_DONE
            msg = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            tag = status.Get_tag()

            if tag == TAG_RESULT and isinstance(msg, tuple):
                rec, x = msg
                if rec is not None and x in self.data:
                    self.data[x]["processed"] = True
                elif x in self.data:
                    self.data[x]["processed"] = False
                # Always advance the progress bar, even on failure
                processed_count += 1
                pbar.update(1)
            elif tag == TAG_DONE:
                active_workers -= 1
                logger.debug(f"Worker {source} finished. {active_workers} remaining.")

            if self.stop_requested:
                break

        pbar.close()
        # Final restart save
        self._save_restart_file()

        elapsed = time.time() - start_time
        logger.info("Manager complete. Merging results...")
        self._final_merge(elapsed)

    def _worker_loop(self) -> None:
        """Rank > 0 Processing loop (Hybrid RMA)."""
        s3_client = self.get_s3_client()

        recs = []
        all_atoms = []
        num_tasks = len(self.indices_to_process)
        pending_indices = list(self.indices_to_process)

        # RMA buffer for Fetch_and_op
        inc_v = np.array(1, dtype="int64")
        res_v = np.array(0, dtype="int64")

        try:
            while not self.stop_requested:
                # One-sided fetch of next task index
                self.win.Lock(0)
                self.win.Fetch_and_op(inc_v, res_v, 0, 0, MPI.SUM)
                self.win.Unlock(0)

                idx_local = int(res_v)
                if idx_local >= num_tasks:
                    break

                idx = pending_indices[idx_local]

                # Process
                res = self.process_single(idx, s3_client=s3_client)
                if res:
                    rec, atoms, x = res
                    # Send only (rec, x) to manager to keep message small
                    self.comm.send((rec, x), dest=0, tag=TAG_RESULT)
                    recs.append(rec)
                    all_atoms.append(atoms)
                    if len(recs) >= self.batch_size:
                        self.flush_recs(recs, all_atoms)
                        recs = []
                        all_atoms = []
                else:
                    x = self.prefixes[idx]
                    self.comm.send((None, x), dest=0, tag=TAG_RESULT)

            # Final flush and signal manager
            if recs:
                self.flush_recs(recs, all_atoms)
            logger.info(f"Worker {self.rank} sending TAG_DONE signal.")
            self.comm.send("DONE", dest=0, tag=TAG_DONE)
        except Exception as e:
            logger.error(f"Worker {self.rank} crashed: {e}", exc_info=True)
            self.comm.send("DONE", dest=0, tag=TAG_DONE)

    def _save_restart_file(self) -> None:
        """Atomically saves current state of self.data to self.restart_file."""
        if getattr(self, "rank", 0) != 0:
            return
        tmp_file = self.restart_file.with_name(f"{self.restart_file.name}.tmp")
        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                json_dump(self.data, f, indent=4)
            tmp_file.replace(self.restart_file)
        except Exception as e:
            logger.error(f"Failed to write restart file {self.restart_file}: {e}", exc_info=True)
            tmp_file.unlink(missing_ok=True)

    def _final_merge(self, elapsed_time: float) -> None:
        """Merges per-rank part files into single Parquet and ExtXYZ outputs."""
        had_merge_error = False
        failed_keys = set()
        is_xyz_only = False

        # Keys present before this merge (if restart and final files existed)
        existing_pq_keys = set()
        existing_xyz_keys = set()

        # --- Parquet merge ---
        all_dfs = []
        if self.restart and self.output_props_final.exists():
            try:
                prev_df = pd.read_parquet(self.output_props_final)
                all_dfs.append(prev_df)
                if "argonne_rel" in prev_df.columns:
                    existing_pq_keys = set(prev_df["argonne_rel"].dropna().unique())
            except Exception as e:
                logger.warning(f"Could not read existing Parquet for restart: {e}")
                had_merge_error = True

        part_pattern = re.compile(
            rf"^props_{re.escape(self.group_name)}_(\d+)_part_(\d+)\.parquet$"
        )
        xyz_pattern = re.compile(
            rf"^structs_{re.escape(self.group_name)}_(\d+)_part_(\d+)\.xyz$"
        )

        part_files = [
            pf
            for pf in sorted(self.output_dir.glob(f"props_{self.group_name}_*.parquet"))
            if pf != self.output_props_final
            and not pf.name.endswith(".tmp")
            and part_pattern.match(pf.name)
        ]
        successful_pfs = []
        for pf in tqdm(part_files, desc="Merging Parquet parts", disable=len(part_files) == 0):
            try:
                df_part = pd.read_parquet(pf)
                all_dfs.append(df_part)
                successful_pfs.append(pf)
            except Exception as e:
                had_merge_error = True
                logger.warning(f"Merge error for {pf}: {e}")
                # Try to recover keys from partner ExtXYZ
                partner_xyz = pf.parent / pf.name.replace("props_", "structs_").replace(".parquet", ".xyz")
                if partner_xyz.exists():
                    try:
                        partner_atoms = safe_read_extxyz(partner_xyz)
                        for at in partner_atoms:
                            k = at.info.get("argonne_rel")
                            if k:
                                failed_keys.add(k)
                    except Exception:
                        pass
                    partner_xyz.unlink(missing_ok=True)
                pf.unlink(missing_ok=True)

        # --- ExtXYZ merge ---
        output_xyz_final = getattr(
            self,
            "output_xyz_final",
            self.output_dir / f"structs_{self.group_name}.xyz",
        )
        xyz_parts = [
            xf
            for xf in sorted(self.output_dir.glob(f"structs_{self.group_name}_*.xyz"))
            if xf != output_xyz_final
            and not xf.name.endswith(".tmp")
            and xyz_pattern.match(xf.name)
        ]

        if not all_dfs and xyz_parts:
            # Fast ExtXYZ streaming when no Parquet files are present
            prefix = getattr(self, "merge_prefix", "stage1") or "stage1"
            prefix_clean = prefix.strip().rstrip("_")
            output_xyz_final = self.output_dir / f"{prefix_clean}_structs_{self.group_name}.xyz"
            logger.warning(
                f"No Parquet part files found for {self.group_name}. "
                f"Merging ExtXYZ parts without Parquet files, adding prefix '{prefix_clean}' to merged file: {output_xyz_final.name}"
            )
            seen_keys = set()
            saved_keys = set()
            dropped_keys = set()
            total_structures = 0
            total_dropped = 0
            successful_xfs = []
            tmp_xyz = self.output_dir / f"{output_xyz_final.name}.tmp"
            xyz_write_success = False
            try:
                chunk_keys = set()
                if self.restart and output_xyz_final.exists():
                    for xf in xyz_parts:
                        chunk_keys.update(fast_extract_keys_from_extxyz_file(xf))

                with open(tmp_xyz, "w", encoding="utf-8", buffering=1024 * 1024) as f_out:
                    if self.restart and output_xyz_final.exists():
                        c, d = fast_stream_extxyz_file(
                            output_xyz_final,
                            f_out,
                            seen_keys,
                            saved_keys,
                            skip_keys=chunk_keys,
                            dropped_keys=dropped_keys,
                        )
                        total_structures += c
                        total_dropped += d
                    for xf in tqdm(
                        xyz_parts,
                        desc=f"Streaming ExtXYZ parts ({prefix_clean})",
                        disable=len(xyz_parts) == 0,
                    ):
                        c, d = fast_stream_extxyz_file(
                            xf,
                            f_out,
                            seen_keys,
                            saved_keys,
                            dropped_keys=dropped_keys,
                        )
                        total_structures += c
                        total_dropped += d
                        successful_xfs.append(xf)

                tmp_xyz.replace(output_xyz_final)
                xyz_write_success = True
                logger.info(
                    f"Merged ExtXYZ into {output_xyz_final} ({total_structures} structures, {total_dropped} dropped frames)"
                )
            except Exception as e:
                had_merge_error = True
                logger.error(f"Failed to stream final ExtXYZ {output_xyz_final}: {e}", exc_info=True)
                tmp_xyz.unlink(missing_ok=True)

            if xyz_write_success:
                for xf in successful_xfs:
                    xf.unlink(missing_ok=True)
                final_saved_keys = saved_keys
            else:
                final_saved_keys = set()

            for k in final_saved_keys:
                if k in self.data:
                    self.data[k]["processed"] = True

            for x, item in self.data.items():
                if item.get("processed", False) and x not in final_saved_keys:
                    failed_keys.add(x)

            if failed_keys or dropped_keys or had_merge_error:
                for k in (failed_keys | dropped_keys):
                    if k in self.data:
                        logger.warning(
                            f"Setting processed=False for '{k}' in restart file due to merge error."
                        )
                        self.data[k]["processed"] = False
                self._save_restart_file()
            return

        all_atoms = []
        if self.restart and output_xyz_final.exists():
            try:
                prev_atoms = safe_read_extxyz(output_xyz_final)
                all_atoms.extend(prev_atoms)
                existing_xyz_keys = {
                    at.info.get("argonne_rel")
                    for at in prev_atoms
                    if at.info.get("argonne_rel") is not None
                }
            except Exception as e:
                logger.warning(f"Could not read existing XYZ for restart: {e}")
                had_merge_error = True

        successful_xfs = []
        for xf in tqdm(xyz_parts, desc="Merging ExtXYZ parts", disable=len(xyz_parts) == 0):
            try:
                atoms_part = safe_read_extxyz(xf)
                all_atoms.extend(atoms_part)
                successful_xfs.append(xf)
            except Exception as e:
                had_merge_error = True
                logger.warning(f"XYZ merge error for {xf}: {e}")
                partner_pq = xf.parent / xf.name.replace("structs_", "props_").replace(".xyz", ".parquet")
                if partner_pq.exists():
                    try:
                        df_partner = pd.read_parquet(partner_pq, columns=["argonne_rel"])
                        for k in df_partner["argonne_rel"].dropna().unique():
                            failed_keys.add(k)
                    except Exception:
                        pass
                    partner_pq.unlink(missing_ok=True)
                xf.unlink(missing_ok=True)

        if all_dfs:
            try:
                final_df = pd.concat(all_dfs, ignore_index=True)
            except Exception as e:
                had_merge_error = True
                logger.error(f"Concatenation error during merge: {e}", exc_info=True)
                final_df = pd.DataFrame()
        else:
            final_df = pd.DataFrame()

        pq_keys = (
            set(final_df["argonne_rel"].dropna().unique())
            if "argonne_rel" in final_df.columns
            else set()
        )
        xyz_keys = {
            at.info.get("argonne_rel")
            for at in all_atoms
            if at.info.get("argonne_rel") is not None
        }

        # Keep only structures that exist in both outputs to maintain strict consistency
        valid_keys = pq_keys & xyz_keys
        mismatched_keys = pq_keys ^ xyz_keys
        if mismatched_keys:
            had_merge_error = True
            failed_keys.update(mismatched_keys)
            logger.warning(
                f"{len(mismatched_keys)} structures mismatched between Parquet and ExtXYZ during merge."
            )
        if "argonne_rel" in final_df.columns:
            if len(pq_keys) != len(valid_keys) or len(final_df) != len(valid_keys):
                final_df = final_df[final_df["argonne_rel"].isin(valid_keys)].drop_duplicates(
                    subset=["argonne_rel"], keep="last"
                )
            # Align all_atoms to the exact valid_keys and sequence of final_df["argonne_rel"]
            atoms_by_key = {}
            for at in all_atoms:
                rel = at.info.get("argonne_rel")
                if rel in valid_keys:
                    atoms_by_key[rel] = at
            all_atoms = [atoms_by_key[k] for k in final_df["argonne_rel"] if k in atoms_by_key]
            assert list(final_df["argonne_rel"]) == [at.info.get("argonne_rel") for at in all_atoms]
        else:
            all_atoms = []

        pq_write_success = False
        if not final_df.empty:
            try:
                final_df.to_parquet(self.output_props_final, index=False)
                logger.info(f"Merged Parquet into {self.output_props_final}")
                pq_write_success = True
                if "process_time_s" in final_df.columns:
                    n = len(final_df)
                    logger.info(f"Throughput: {n / elapsed_time:.2f} files/s")
            except Exception as e:
                had_merge_error = True
                logger.error(f"Failed to write final Parquet {self.output_props_final}: {e}", exc_info=True)
                failed_keys.update(valid_keys - existing_pq_keys)
        else:
            if not is_xyz_only and (not self.restart or not self.output_props_final.exists()):
                logger.warning("No data processed.")

        xyz_write_success = False
        if all_atoms:
            try:
                write(
                    str(output_xyz_final),
                    tqdm(all_atoms, desc="Writing merged ExtXYZ", disable=len(all_atoms) == 0),
                    format="extxyz",
                )
                logger.info(
                    f"Merged ExtXYZ into {output_xyz_final} ({len(all_atoms)} structures)"
                )
                xyz_write_success = True
            except Exception as e:
                had_merge_error = True
                logger.error(f"Failed to write final ExtXYZ {output_xyz_final}: {e}", exc_info=True)
                failed_keys.update(valid_keys - existing_xyz_keys)

        # Unlink successfully merged part files only if writes succeeded
        if is_xyz_only and xyz_write_success:
            for xf in successful_xfs:
                xf.unlink(missing_ok=True)
            final_saved_keys = xyz_keys
        elif pq_write_success and xyz_write_success:
            for pf in successful_pfs:
                pf.unlink(missing_ok=True)
            for xf in successful_xfs:
                xf.unlink(missing_ok=True)
            final_saved_keys = valid_keys
        else:
            final_saved_keys = existing_pq_keys & existing_xyz_keys

        for k in final_saved_keys:
            if k in self.data:
                self.data[k]["processed"] = True

        # Identify any key marked processed in self.data that is NOT in the final saved outputs
        for x, item in self.data.items():
            if item.get("processed", False) and x not in final_saved_keys:
                failed_keys.add(x)

        # Reset processed=False for any key that encountered an error during merge
        if failed_keys:
            for k in failed_keys:
                if k in self.data:
                    logger.warning(
                        f"Setting processed=False for '{k}' in restart file due to merge error."
                    )
                    self.data[k]["processed"] = False
            self._save_restart_file()
        elif had_merge_error:
            self._save_restart_file()

    def run_mpi(self) -> None:
        """Main entry point for MPI runs (Hybrid RMA)."""
        self.comm = MPI.COMM_WORLD
        self.rank = self.comm.Get_rank()
        self.size = self.comm.Get_size()

        if self.size < 2:
            logger.warning("MPI size < 2. Falling back to serial.")
            self.run_serial()
            return

        # Prepare RMA Window for task counter (Rank 0 hosts it)
        # Allocate 8 bytes (int64)
        itemsize = MPI.INT64_T.Get_size()
        nbytes = itemsize if self.rank == 0 else 0
        self.win = MPI.Win.Allocate(nbytes, itemsize, comm=self.comm)

        if self.rank == 0:
            # Initialize counter to 0
            mem = self.win.tomemory()
            mem[0:8] = np.int64(0).tobytes()

        self.comm.Barrier()  # Sync after window creation

        try:
            if self.rank == 0:
                self._manager_loop()
            else:
                self._worker_loop()
        except Exception as e:
            logger.error(f"Rank {self.rank} encountered error in run_mpi: {e}", exc_info=True)
            raise
        finally:
            self.win.Free()
            logger.info(f"Rank {self.rank} exiting clean.")

    def run_serial(self) -> None:
        """Standard serial fallback (Synchronous)."""
        start_time = time.time()
        s3_client = self.get_s3_client()

        try:
            recs = []
            all_atoms = []
            for idx in tqdm(self.indices_to_process, desc="Serial"):
                if self.stop_requested:
                    break
                res = self.process_single(idx, s3_client)
                if res:
                    rec, atoms, x = res
                    recs.append(rec)
                    all_atoms.append(atoms)
                    self.data[x]["processed"] = True
                    if len(recs) >= self.batch_size:
                        self.flush_recs(recs, all_atoms)
                        recs = []
                        all_atoms = []
                else:
                    x = self.prefixes[idx]
                    if x in self.data:
                        self.data[x]["processed"] = False

            if recs:
                self.flush_recs(recs, all_atoms)
            self._save_restart_file()
            self._final_merge(time.time() - start_time)
        except Exception as e:
            logger.error(f"Error during serial execution: {e}", exc_info=True)
            raise


def merge_chunks(
    data_source: Union[str, Path],
    output_dir: Union[str, Path],
    delete_chunks: bool = False,
    prefix: str = "stage1",
) -> Tuple[Optional[Path], Optional[Path], Optional[Path]]:
    """
    Merges all completed chunk and part files in output_dir into unified final outputs
    (props_{base}.parquet and structs_{base}.xyz).
    Supports:
      - Range chunk files: props_{base}_{start}_{end}.parquet
      - Range part files: props_{base}_{start}_{end}_{rank}_part_{part}.parquet
      - Rank part files (from runs without start-index): props_{base}_{rank}_part_{part}.parquet
      - ExtXYZ-only merges (when no Parquet files exist): prints a warning and adds
        a prefix (e.g. stage1_structs_{base}.xyz) to the merged output.
    Also consolidates all chunk restart files into {stem}_restart.json and marks merged
    records as processed=True.

    Parameters
    ----------
    data_source : Union[str, Path]
        Path to the original prefix JSON file (e.g. omol_prefix.json or rna_prefix.json).
    output_dir : Union[str, Path]
        Directory where chunk/part parquet and xyz files are located.
    delete_chunks : bool, default False
        Whether to delete individual intermediate chunk/part files after successful merge.
    prefix : str, default "stage1"
        Prefix to add to merged ExtXYZ file when merging without Parquet files.

    Returns
    -------
    Tuple[Optional[Path], Optional[Path], Optional[Path]]
        Paths to (final_parquet, final_xyz, final_restart_json).
        If merging without Parquet files, final_parquet will be None.
    """
    data_source = Path(data_source)
    output_dir = Path(output_dir)

    stem = data_source.stem
    stem_no_restart = stem[:-8] if stem.endswith("_restart") else stem
    m_chunk = re.search(r"^(.*?)(?:_(\d+)_(\d+))$", stem_no_restart)
    stem_base = m_chunk.group(1) if m_chunk else stem_no_restart
    base_group = stem_base.replace("_prefix", "")

    final_pq = output_dir / f"props_{base_group}.parquet"
    final_xyz = output_dir / f"structs_{base_group}.xyz"

    re_range_chunk = re.compile(
        rf"^props_{re.escape(base_group)}_(\d+)_(\d+)\.parquet$"
    )
    re_range_part = re.compile(
        rf"^props_{re.escape(base_group)}_(\d+)_(\d+)_(\d+)_part_(\d+)\.parquet$"
    )
    re_rank_part = re.compile(
        rf"^props_{re.escape(base_group)}_(\d+)_part_(\d+)\.parquet$"
    )
    re_any_props = re.compile(
        rf"^props_{re.escape(base_group)}_.*\.parquet$"
    )

    if not output_dir.exists():
        logger.error(f"Output directory {output_dir} does not exist.")
        return None, None, None

    items = []
    dropped_keys = set()
    for pf in output_dir.iterdir():
        if not pf.is_file() or pf.name.endswith(".tmp") or pf == final_pq:
            continue

        m1 = re_range_chunk.match(pf.name)
        if m1:
            start, end = int(m1.group(1)), int(m1.group(2))
            sort_key = (0, start, end, 0, 0, pf.name)
            is_range_chunk = True
        else:
            m2 = re_range_part.match(pf.name)
            if m2:
                start, end, rank, part = int(m2.group(1)), int(m2.group(2)), int(m2.group(3)), int(m2.group(4))
                sort_key = (1, start, end, rank, part, pf.name)
                is_range_chunk = False
            else:
                m3 = re_rank_part.match(pf.name)
                if m3:
                    rank, part = int(m3.group(1)), int(m3.group(2))
                    start, end = 0, 0
                    sort_key = (2, 0, 0, rank, part, pf.name)
                    is_range_chunk = False
                else:
                    if re_any_props.match(pf.name):
                        start, end = 0, 0
                        sort_key = (3, 0, 0, 0, 0, pf.name)
                        is_range_chunk = False
                    else:
                        continue

        xf = pf.parent / pf.name.replace("props_", "structs_").replace(".parquet", ".xyz")
        if not xf.exists():
            logger.warning(
                f"Missing ExtXYZ partner {xf.name} for chunk Parquet {pf.name}. "
                f"Dropping {pf.name} and continuing with the rest that match."
            )
            try:
                df_orphan = pd.read_parquet(pf, columns=["argonne_rel"])
                if "argonne_rel" in df_orphan.columns:
                    dropped_keys.update(df_orphan["argonne_rel"].dropna().unique())
            except Exception:
                pass
            pf.unlink(missing_ok=True)
            continue

        items.append({
            "pf": pf,
            "xf": xf,
            "start": start,
            "end": end,
            "sort_key": sort_key,
            "is_range_chunk": is_range_chunk,
        })

    if not items:
        # Check if ExtXYZ chunk/part files exist to allow merging without Parquet files
        re_xyz_range_chunk = re.compile(
            rf"^structs_{re.escape(base_group)}_(\d+)_(\d+)\.xyz$"
        )
        re_xyz_range_part = re.compile(
            rf"^structs_{re.escape(base_group)}_(\d+)_(\d+)_(\d+)_part_(\d+)\.xyz$"
        )
        re_xyz_rank_part = re.compile(
            rf"^structs_{re.escape(base_group)}_(\d+)_part_(\d+)\.xyz$"
        )
        re_xyz_any = re.compile(
            rf"^structs_{re.escape(base_group)}_.*\.xyz$"
        )

        prefix_clean = prefix.strip().rstrip("_") if prefix else "stage1"
        target_xyz_name = f"{prefix_clean}_structs_{base_group}.xyz"
        final_xyz = output_dir / target_xyz_name

        xyz_items = []
        for xf in output_dir.iterdir():
            if (
                not xf.is_file()
                or xf.name.endswith(".tmp")
                or xf == final_xyz
                or xf.name == f"structs_{base_group}.xyz"
            ):
                continue

            m1 = re_xyz_range_chunk.match(xf.name)
            if m1:
                start, end = int(m1.group(1)), int(m1.group(2))
                sort_key = (0, start, end, 0, 0, xf.name)
            else:
                m2 = re_xyz_range_part.match(xf.name)
                if m2:
                    start, end, rank, part = int(m2.group(1)), int(m2.group(2)), int(m2.group(3)), int(m2.group(4))
                    sort_key = (1, start, end, rank, part, xf.name)
                else:
                    m3 = re_xyz_rank_part.match(xf.name)
                    if m3:
                        rank, part = int(m3.group(1)), int(m3.group(2))
                        sort_key = (2, 0, 0, rank, part, xf.name)
                    else:
                        if re_xyz_any.match(xf.name):
                            sort_key = (3, 0, 0, 0, 0, xf.name)
                        else:
                            continue
            xyz_items.append({
                "xf": xf,
                "sort_key": sort_key,
            })

        if not xyz_items:
            other_props = [
                f.name for f in output_dir.glob("props_*.parquet")
                if f.is_file() and not f.name.endswith(".tmp") and f != final_pq
            ]
            msg = f"No chunk or part Parquet or ExtXYZ files found in {output_dir} matching props_{base_group}_*.parquet or structs_{base_group}_*.xyz"
            if other_props:
                msg += f" (found other Parquet files: {other_props[:5]}...; check if --data-source matches the expected group '{base_group}')."
            logger.warning(msg)
            return None, None, None

        logger.warning(
            f"No Parquet chunk files found matching props_{base_group}_*.parquet in {output_dir}. "
            f"Merging without Parquet files (ExtXYZ only). "
            f"Adding prefix '{prefix_clean}' to merged file: {final_xyz.name}"
        )

        xyz_items.sort(key=lambda x: x["sort_key"])

        seen_keys = set()
        saved_keys = set()
        dropped_keys = set()
        total_structures = 0
        total_dropped = 0
        tmp_xyz = output_dir / f"{final_xyz.name}.tmp"

        try:
            chunk_keys = set()
            if final_xyz.exists():
                for it in xyz_items:
                    chunk_keys.update(fast_extract_keys_from_extxyz_file(it["xf"]))

            with open(tmp_xyz, "w", encoding="utf-8", buffering=1024 * 1024) as f_out:
                if final_xyz.exists():
                    c, d = fast_stream_extxyz_file(
                        final_xyz,
                        f_out,
                        seen_keys,
                        saved_keys,
                        skip_keys=chunk_keys,
                        dropped_keys=dropped_keys,
                    )
                    total_structures += c
                    total_dropped += d
                for it in tqdm(
                    xyz_items,
                    desc=f"Streaming ExtXYZ ({prefix_clean})",
                    disable=len(xyz_items) == 0,
                ):
                    c, d = fast_stream_extxyz_file(
                        it["xf"],
                        f_out,
                        seen_keys,
                        saved_keys,
                        dropped_keys=dropped_keys,
                    )
                    total_structures += c
                    total_dropped += d

            tmp_xyz.replace(final_xyz)
            logger.info(
                f"Successfully merged {len(xyz_items)} ExtXYZ chunks/parts into {final_xyz} "
                f"({total_structures} structures, {total_dropped} dropped frames)."
            )
        except Exception as e:
            logger.error(f"Failed to merge ExtXYZ files into {final_xyz}: {e}", exc_info=True)
            tmp_xyz.unlink(missing_ok=True)
            raise

        # Consolidate restart files
        chunk_rst_pattern = re.compile(
            rf"^{re.escape(stem_base)}_(\d+)_(\d+)_restart\.json$"
        )
        candidate_dirs = {data_source.parent, output_dir, Path(".")}
        found_rsts = []
        for c_dir in candidate_dirs:
            if c_dir.exists():
                for rf in c_dir.iterdir():
                    if rf.is_file() and chunk_rst_pattern.match(rf.name):
                        found_rsts.append(rf)

        final_rst = data_source.parent / f"{stem_base}_restart.json"
        base_data = {}
        if final_rst.exists():
            try:
                with open(final_rst, "r", encoding="utf-8") as f:
                    base_data = json_load(f)
            except Exception as e:
                logger.warning(f"Could not read existing restart file {final_rst}: {e}")
        elif data_source.exists() and data_source.is_file():
            try:
                with open(data_source, "r", encoding="utf-8") as f:
                    base_data = json_load(f)
            except Exception as e:
                logger.warning(f"Could not load base data source {data_source}: {e}")

        for rf in sorted(found_rsts):
            try:
                with open(rf, "r", encoding="utf-8") as f:
                    c_data = json_load(f)
                for k, v in c_data.items():
                    if k in base_data and isinstance(v, dict):
                        base_data[k].update(v)
                    else:
                        base_data[k] = v
            except Exception as e:
                logger.warning(f"Could not read chunk restart file {rf}: {e}")

        for k in saved_keys:
            if k in base_data and isinstance(base_data[k], dict):
                base_data[k]["processed"] = True

        for k in dropped_keys:
            if k in base_data and isinstance(base_data[k], dict) and k not in saved_keys:
                logger.warning(
                    f"Setting processed=False for '{k}' in consolidated restart file due to corrupted/truncated frame."
                )
                base_data[k]["processed"] = False

        if base_data:
            tmp_rst = data_source.parent / f"{stem_base}_restart.json.tmp"
            try:
                with open(tmp_rst, "w", encoding="utf-8") as f:
                    json_dump(base_data, f, indent=4)
                tmp_rst.replace(final_rst)
                logger.info(f"Consolidated restart file saved to {final_rst}")
            except Exception as e:
                logger.error(f"Failed to write consolidated restart file: {e}", exc_info=True)
                tmp_rst.unlink(missing_ok=True)
        else:
            final_rst = None

        if delete_chunks:
            logger.info("Deleting intermediate ExtXYZ chunk and part files...")
            for it in xyz_items:
                it["xf"].unlink(missing_ok=True)
            for rf in found_rsts:
                rf.unlink(missing_ok=True)

        return None, final_xyz, final_rst

    # Sort items in logical processing order
    items.sort(key=lambda x: x["sort_key"])

    # Check for gaps or overlaps if all items are range chunks
    if all(it["is_range_chunk"] for it in items) and len(items) > 1:
        for i in range(len(items) - 1):
            cur = items[i]
            nxt = items[i + 1]
            if cur["end"] < nxt["start"]:
                logger.warning(
                    f"Gap detected between chunk [{cur['start']}, {cur['end']}) and [{nxt['start']}, {nxt['end']}): {nxt['start'] - cur['end']} entries missing."
                )
            elif cur["end"] > nxt["start"]:
                logger.warning(
                    f"Overlap detected between chunk [{cur['start']}, {cur['end']}) and [{nxt['start']}, {nxt['end']})."
                )

    # --- Merge Parquet and ExtXYZ files with strict consistency ---
    all_dfs = []
    if final_pq.exists():
        try:
            prev_df = pd.read_parquet(final_pq)
            all_dfs.append(prev_df)
        except Exception as e:
            logger.warning(f"Could not read existing final Parquet {final_pq}: {e}")

    for it in tqdm(items, desc="Merging Parquet chunks/parts"):
        pf = it["pf"]
        try:
            df = pd.read_parquet(pf)
            all_dfs.append(df)
        except Exception as e:
            logger.error(f"Failed to read Parquet chunk/part {pf}: {e}", exc_info=True)
            raise

    raw_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
    pq_keys = (
        set(raw_df["argonne_rel"].dropna().unique())
        if "argonne_rel" in raw_df.columns
        else set()
    )

    all_atoms = []
    if final_xyz.exists():
        try:
            prev_atoms = safe_read_extxyz(final_xyz)
            all_atoms.extend(prev_atoms)
        except Exception as e:
            logger.warning(f"Could not read existing final ExtXYZ {final_xyz}: {e}")

    xyz_files = [it["xf"] for it in items]
    for xf in tqdm(xyz_files, desc="Reading ExtXYZ chunks/parts"):
        try:
            atoms_part = safe_read_extxyz(xf)
            all_atoms.extend(atoms_part)
        except Exception as e:
            logger.warning(f"Failed to read ExtXYZ file {xf}: {e}. Skipping unreadable frames.")

    xyz_keys = {
        at.info.get("argonne_rel")
        for at in all_atoms
        if at.info.get("argonne_rel") is not None
    }

    # Strict consistency: keep only structures present in both Parquet and ExtXYZ
    valid_keys = pq_keys & xyz_keys
    mismatched_keys = pq_keys ^ xyz_keys
    if mismatched_keys:
        pq_only = pq_keys - xyz_keys
        xyz_only = xyz_keys - pq_keys
        sample_pq = list(pq_only)[:3]
        sample_xyz = list(xyz_only)[:3]
        logger.warning(
            f"Merge mismatch detected: {len(mismatched_keys)} structures not present in both outputs "
            f"({len(pq_only)} only in Parquet e.g. {sample_pq}, {len(xyz_only)} only in ExtXYZ e.g. {sample_xyz}). "
            f"Pruning to common intersection of {len(valid_keys)} structures."
        )

    if "argonne_rel" in raw_df.columns:
        final_df = raw_df[raw_df["argonne_rel"].isin(valid_keys)].drop_duplicates(
            subset=["argonne_rel"], keep="last"
        )
        atoms_by_key = {}
        for at in all_atoms:
            rel = at.info.get("argonne_rel")
            if rel in valid_keys:
                atoms_by_key[rel] = at
        final_atoms = [atoms_by_key[k] for k in final_df["argonne_rel"] if k in atoms_by_key]

        final_pq_keys = list(final_df["argonne_rel"])
        final_xyz_keys = [at.info.get("argonne_rel") for at in final_atoms]
        if final_pq_keys != final_xyz_keys:
            raise RuntimeError(
                f"Parquet and ExtXYZ argonne_rel sequence mismatch after merge: {len(final_pq_keys)} vs {len(final_xyz_keys)}"
            )
    else:
        final_df = raw_df
        final_atoms = []

    tmp_pq = output_dir / f"props_{base_group}.parquet.tmp"
    final_df.to_parquet(tmp_pq, index=False)

    tmp_xyz = output_dir / f"structs_{base_group}.xyz.tmp"
    write(str(tmp_xyz), final_atoms, format="extxyz")

    tmp_pq.replace(final_pq)
    tmp_xyz.replace(final_xyz)
    logger.info(
        f"Successfully merged {len(items)} chunks/parts into {final_pq} and {final_xyz} "
        f"({len(final_df)} consistent structures with identical argonne_rel)."
    )

    # --- Consolidate Restart Files ---
    chunk_rst_pattern = re.compile(
        rf"^{re.escape(stem_base)}_(\d+)_(\d+)_restart\.json$"
    )
    candidate_dirs = {data_source.parent, output_dir, Path(".")}
    found_rsts = []
    for c_dir in candidate_dirs:
        if c_dir.exists():
            for rf in c_dir.iterdir():
                if rf.is_file() and chunk_rst_pattern.match(rf.name):
                    found_rsts.append(rf)

    final_rst = data_source.parent / f"{stem_base}_restart.json"
    base_data = {}
    if final_rst.exists():
        try:
            with open(final_rst, "r", encoding="utf-8") as f:
                base_data = json_load(f)
        except Exception as e:
            logger.warning(f"Could not read existing restart file {final_rst}: {e}")
    elif data_source.exists() and data_source.is_file():
        try:
            with open(data_source, "r", encoding="utf-8") as f:
                base_data = json_load(f)
        except Exception as e:
            logger.warning(f"Could not load base data source {data_source}: {e}")

    for rf in sorted(found_rsts):
        try:
            with open(rf, "r", encoding="utf-8") as f:
                c_data = json_load(f)
            for k, v in c_data.items():
                if k in base_data and isinstance(v, dict):
                    base_data[k].update(v)
                else:
                    base_data[k] = v
        except Exception as e:
            logger.warning(f"Could not read chunk restart file {rf}: {e}")

    # Mark all merged structures as processed
    if "argonne_rel" in final_df.columns:
        for k in final_df["argonne_rel"].dropna().unique():
            if k in base_data and isinstance(base_data[k], dict):
                base_data[k]["processed"] = True

    # Reset any mismatched or dropped structures to processed=False so they can be re-processed
    unmerged_keys = mismatched_keys | dropped_keys
    if unmerged_keys:
        for k in unmerged_keys:
            if k in base_data and isinstance(base_data[k], dict):
                logger.warning(
                    f"Setting processed=False for '{k}' in consolidated restart file due to Parquet/ExtXYZ mismatch or missing partner."
                )
                base_data[k]["processed"] = False

    if base_data:
        tmp_rst = data_source.parent / f"{stem_base}_restart.json.tmp"
        try:
            with open(tmp_rst, "w", encoding="utf-8") as f:
                json_dump(base_data, f, indent=4)
            tmp_rst.replace(final_rst)
            logger.info(f"Consolidated restart file saved to {final_rst}")
        except Exception as e:
            logger.error(f"Failed to write consolidated restart file: {e}", exc_info=True)
            tmp_rst.unlink(missing_ok=True)
    else:
        final_rst = None

    # --- Cleanup if requested ---
    if delete_chunks:
        logger.info("Deleting intermediate chunk and part files...")
        for it in items:
            it["pf"].unlink(missing_ok=True)
            it["xf"].unlink(missing_ok=True)
        for rf in found_rsts:
            rf.unlink(missing_ok=True)

    return final_pq, final_xyz, final_rst

