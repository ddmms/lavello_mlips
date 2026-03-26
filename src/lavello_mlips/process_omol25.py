import argparse
import hashlib
import logging
import os
import re
import signal
import sys
import time
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import numpy as np
import pandas as pd
from ase.io import read, write
from botocore.config import Config
from mpi4py import MPI
from tarfile import open as tar_open
from tqdm import tqdm
from zstandard import ZstdDecompressor

import ase.parallel
from ase.parallel import DummyMPI
ase.parallel.world = DummyMPI()

from .s3_processor import S3DataProcessor
from .utils import setup_logging, json_load, json_dump

# ---------- constants ----------
AU2D = 2.541746  # a.u. → Debye
D2EA = 0.20819434  # Debye → e·Å
AU2B = 1.34503431  # a.u. → Buckingham
AU2EAA = 0.2800285205359031  # a.u. → e·Å²
B2EAA = 0.208194333  # Buckingham → e·Å²

MiB = 1024**2
GiB = 1024 * MiB

# Tags for MPI communication
TAG_READY = 1
TAG_DONE = 2
TAG_STOP = 3
TAG_TASK = 4
TAG_RESULT = 5

logger = logging.getLogger(__name__)

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
        if unit == "a.u.":
            x, y, z = x * AU2D, y * AU2D, z * AU2D
        best = (x, y, z)
        if unit == "debye":
            break  # prefer explicit Debye value
    if not best:
        return None
    Dx, Dy, Dz = best
    Mag = (Dx * Dx + Dy * Dy + Dz * Dz) ** 0.5
    return Dx, Dy, Dz, Mag


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
                logger.debug("Failed to parse charge value from match %r in text; ignoring.", q_match)
        else:
            m_match = m.group(2)
            if m_match is not None:
                try:
                    M = int(m_match)
                except ValueError:
                    # Ignore unparsable multiplicity value; leave M as-is (None or previous match).
                    logger.debug("Failed to parse multiplicity value from match %r in text; ignoring.", m_match)

    m = RE_XYZ.search(txt)
    if m:
        try:
            Q = int(m.group(1))
            M = int(m.group(2))
        except ValueError:
            # Ignore unparsable XYZ header values; leave Q/M as determined above.
            logger.debug(
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
    l = min(virt_above) if virt_above else None
    return evals[h], (evals[l] if l is not None else None)


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
            if "ALPHA" in back:
                tag = "A"
            if "BETA" in back:
                tag = "B"
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
                "homo_a_eV": ha,
                "lumo_a_eV": la,
                "gap_a_eV": (None if ha is None or la is None else la - ha),
                "homo_b_eV": hb,
                "lumo_b_eV": lb,
                "gap_b_eV": (None if hb is None or lb is None else lb - hb),
                "homo_eV": max([x for x in [ha, hb] if x is not None], default=None),
                "lumo_eV": min([x for x in [la, lb] if x is not None], default=None),
                "n_orb_a": len(a_eV),
                "n_orb_b": len(b_eV),
            }
        )
        if res["homo_eV"] is not None and res["lumo_eV"] is not None:
            res["gap_eV"] = res["lumo_eV"] - res["homo_eV"]
    elif R:
        r_occ, r_eV = zip(*R) if R else ([], [])
        r_occ, r_eV = list(r_occ), list(r_eV)
        ha, la = homo_lumo(r_eV, r_occ)
        res.update(
            {
                "spin_case": "RKS",
                "homo_eV": ha,
                "lumo_eV": la,
                "gap_eV": (None if ha is None or la is None else la - ha),
                "n_orb_a": len(r_eV),
                "n_orb_b": 0,
            }
        )
    return res


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
        self.group_name = self.files_to_process.stem.replace("_prefix", "").replace(
            "_restart", ""
        )
        self.output_props_final = self.output_dir / f"props_{self.group_name}.parquet"
        self.output_props_rank = (
            self.output_dir / f"props_{self.group_name}_{self.rank}.parquet"
        )

        if self.files_to_process.name.endswith("_restart.json"):
            self.restart_file = self.files_to_process
            self.restart = True
        else:
            self.restart_file = self.files_to_process.with_name(
                self.files_to_process.stem + "_restart.json"
            )

        if self.rank == 0:
            logger.info(f"Initializing dataset from: {self.files_to_process.resolve()}")

        with open(self.files_to_process, "r", encoding="utf-8") as f:
            self.data = json_load(f)

        if self.restart:
            if self.rank == 0:
                logger.info(
                    "Scanning output directory to recover already processed files..."
                )
                recovered_count = 0
                parquet_files = list(
                    self.output_dir.glob(f"props_{self.group_name}*.parquet")
                )

                for pf in parquet_files:
                    xyz_file = pf.parent / pf.name.replace(
                        "props_", "structs_"
                    ).replace(".parquet", ".xyz")

                    try:
                        if not xyz_file.exists():
                            raise FileNotFoundError(
                                f"Missing ExtXYZ partner: {xyz_file.name}"
                            )

                        df = pd.read_parquet(pf, columns=["argonne_rel"])

                        atoms_list = read(str(xyz_file), index=":")
                        if not isinstance(atoms_list, list):
                            atoms_list = [atoms_list] if atoms_list else []

                        if len(df) != len(atoms_list):
                            raise ValueError(
                                f"Length mismatch: {len(df)} Parquet rows vs {len(atoms_list)} XYZ structures"
                            )

                        # If we pass all checks, mark as processed!
                        for x in df["argonne_rel"].dropna().unique():
                            if x in self.data and not self.data[x].get(
                                "processed", False
                            ):
                                self.data[x]["processed"] = True
                                recovered_count += 1

                    except Exception as e:
                        # If a part file is corrupt/orphaned, delete it so _final_merge ignores it
                        logger.warning(
                            f"Discarding corrupt/incomplete part {pf.name}: {e}"
                        )
                        pf.unlink(missing_ok=True)
                        xyz_file.unlink(missing_ok=True)

                if recovered_count > 0:
                    logger.info(
                        f"Recovered {recovered_count} previously processed items from disk, and discarded invalid parts."
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
            self.output_dir.mkdir(parents=True, exist_ok=True)

        if self.sample_size < 0:
            self.sample_size = len(self.prefixes)

        max_idx = len(self.prefixes)
        start_idx = self.args.start_index
        end_idx = min(start_idx + self.sample_size, max_idx)

        self.indices_to_process = list(range(start_idx, end_idx))
        num_tasks = len(self.indices_to_process)

        if getattr(self.args, "batch_size", None) is not None:
            self.batch_size = max(100, self.args.batch_size)
        else:
            active_workers = (
                max(1, self.size - 1) if self.size > 1 else max(1, self.size)
            )
            tasks_per_worker = max(1, num_tasks / active_workers)
            self.batch_size = max(100, int(tasks_per_worker * 0.01))

        if self.rank == 0:
            logger.info(f"Using flush batch size of {self.batch_size}")

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

    def _process_buffer(self, buffer: BytesIO, x: str) -> Optional[Tuple[Dict[str, Any], Any]]:
        """Parse a .tar.zst buffer; returns (rec dict, ASE Atoms) or None."""
        rec: Dict[str, Any] = {}
        try:
            decompressor = ZstdDecompressor()
            decompressed_buffer = BytesIO()
            decompressor.copy_stream(buffer, decompressed_buffer)
            decompressed_buffer.seek(0)

            with tar_open(fileobj=decompressed_buffer, mode="r") as tar:
                out = tar.getmember("orca.out")
                inp = tar.getmember("orca.inp")
                prop = tar.getmember("orca.property.txt")

                txt_out = tar.extractfile(out).read().decode("utf-8")
                txt_prop = tar.extractfile(prop).read().decode("utf-8")
                txt_inp = tar.extractfile(inp).read().decode("utf-8")

            dip = parse_dipole(((txt_prop or "") + "\n" + (txt_out or "")))
            if dip:
                Dx, Dy, Dz, Mag = dip
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
                rec["status_dipole"] = "MISSING"

            quad = parse_quadrupole(txt_out or "")
            if quad:
                xx, yy, zz, xy, xz, yz, q = quad
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
            else:
                rec["status_quadrupole"] = "MISSING"

            Q, M = parse_charge_mult((txt_out or "") + "\n" + (txt_inp or ""))
            rec["total_charge_e"] = Q
            rec["multiplicity"] = M

            atoms = read(StringIO(txt_out), format="orca-output")
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
                    "homo_eV",
                    "lumo_eV",
                    "gap_eV",
                    "homo_a_eV",
                    "lumo_a_eV",
                    "gap_a_eV",
                    "homo_b_eV",
                    "lumo_b_eV",
                    "gap_b_eV",
                    "n_orb_a",
                    "n_orb_b",
                ]:
                    if k in eig:
                        rec[k] = eig[k]
            else:
                rec["status_eigs"] = "MISSING"

            # Embed all properties into atoms.info for ExtXYZ output
            for k, v in rec.items():
                if v is not None:
                    atoms.info[k] = v

            return rec, atoms
        except Exception as e:
            logger.error(f"Error parsing buffer for {x}: {e}")
            return None

    def process_single(self, idx: int, s3_client: Any = None) -> Optional[Tuple[Dict[str, Any], Any, str]]:
        """Processes a single task synchronously. Returns (rec, atoms, x) or None."""
        start_time = time.time()
        x = self.prefixes[idx]
        key_list = self.data[x]["key"]
        if isinstance(key_list, list):
            tar_key = key_list[0]
        else:
            tar_key = key_list
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
                    return None
                content = local_path.read_bytes()
                buffer.write(content)
            else:
                response = s3_client.get_object(Bucket=self.args.bucket, Key=source)
                buffer.write(response["Body"].read())

            buffer.seek(0)
            result = self._process_buffer(buffer, x)
            if result is None:
                return None
            parsed_rec, atoms = result
            rec.update(parsed_rec)

            rec["process_time_s"] = time.time() - start_time
            # Update atoms.info with the final rec (includes argonne_rel, data_id, process_time_s)
            atoms.info["argonne_rel"] = x
            atoms.info["data_id"] = rec["data_id"]
            return rec, atoms, x
        except Exception as e:
            logger.error(f"Error processing {source}: {e}")
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
        with open(self.restart_file, "w") as f:
            json_dump(self.data, f, indent=4)

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
            logger.error(f"Worker {self.rank} crashed: {e}")
            self.comm.send("DONE", dest=0, tag=TAG_DONE)

    def _final_merge(self, elapsed_time: float) -> None:
        """Merges per-rank part files into single Parquet and ExtXYZ outputs."""

        # --- Parquet merge ---
        all_dfs = []
        if self.restart and self.output_props_final.exists():
            all_dfs.append(pd.read_parquet(self.output_props_final))

        part_files = sorted(
            list(self.output_dir.glob(f"props_{self.group_name}_*.parquet"))
        )
        for pf in part_files:
            if pf == self.output_props_final:
                continue
            try:
                all_dfs.append(pd.read_parquet(pf))
                pf.unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"Merge error for {pf}: {e}")

        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            final_df.to_parquet(self.output_props_final, index=False)
            logger.info(f"Merged Parquet into {self.output_props_final}")
            if "process_time_s" in final_df.columns:
                n = len(final_df)
                logger.info(f"Throughput: {n / elapsed_time:.2f} files/s")
        else:
            logger.warning("No data processed.")

        # --- ExtXYZ merge ---
        output_xyz_final = self.output_dir / f"structs_{self.group_name}.xyz"
        all_atoms = []
        if self.restart and output_xyz_final.exists():
            try:
                all_atoms.extend(read(str(output_xyz_final), index=":"))
            except Exception as e:
                logger.warning(f"Could not read existing XYZ for restart: {e}")

        xyz_parts = sorted(
            list(self.output_dir.glob(f"structs_{self.group_name}_*.xyz"))
        )
        for xf in xyz_parts:
            if xf == output_xyz_final:
                continue
            try:
                all_atoms.extend(read(str(xf), index=":"))
                xf.unlink(missing_ok=True)
            except Exception as e:
                logger.warning(f"XYZ merge error for {xf}: {e}")

        if all_atoms:
            write(str(output_xyz_final), all_atoms, format="extxyz")
            logger.info(
                f"Merged ExtXYZ into {output_xyz_final} ({len(all_atoms)} structures)"
            )

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

            if recs:
                self.flush_recs(recs, all_atoms)
            with open(self.restart_file, "w") as f:
                json_dump(self.data, f, indent=4)
            self._final_merge(time.time() - start_time)
        finally:
            pass

logger = logging.getLogger(__name__)
