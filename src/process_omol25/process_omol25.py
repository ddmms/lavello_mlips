import logging
import sys
import os
import signal
from io import BytesIO, StringIO
from json import load as json_load, dump as json_dump
from pathlib import Path
import re
import hashlib
from ase.io import read, write
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from boto3 import client as s3_client
from tarfile import open as tar_open
from zstandard import ZstdDecompressor
from typing import Optional, Dict, Any, List, Tuple
import pandas as pd
import numpy as np
import psutil
from tqdm import tqdm
import time
MiB = 1024 ** 2
GiB = 1024 * MiB
# ... (rest of constants)

multipart_threshold_bytes = 10 * GiB    # 10GB
multipart_chunksize_bytes = 500 * MiB # 500MB
transfer_max_concurrency = 2


s3_transfer_options = TransferConfig(
    multipart_threshold=multipart_threshold_bytes,
    multipart_chunksize=multipart_chunksize_bytes,
    max_concurrency=transfer_max_concurrency,
    num_download_attempts=5,
)
# ---------- constants ----------
AU2D = 2.541746          # a.u. → Debye
D2EA = 0.20819434        # Debye → e·Å
AU2B = 1.34503431        # a.u. → Buckingham
AU2EAA = 0.2800285205359031 # a.u. → e·Å²
B2EAA = 0.208194333 # Buckingham → e·Å²
# ---------- dipole ----------
RE_DIP = re.compile(
    r"Total\s+Dipole\s+Moment(?:\s*\((a\.u\.|Debye)\))?\s*:\s*"
    r"([+-]?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?)\s+"
    r"([+-]?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?)\s+"
    r"([+-]?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?)", re.I)


# ---------- quadrupole ----------
RE_QUAD = re.compile(
    r"TOT\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+\(a\.u\.\)\n"
    r"\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)\s+\(Buckingham\)",
    re.DOTALL
)

def parse_quadrupole (txt: str):
    if not txt: return None
    match = RE_QUAD.search(txt)

    if match:
        vals = [float(g) for g in match.groups()]
        # Buckingham values are the last 6 groups
        xx, yy, zz, xy, xz, yz = vals[6:12]
        q_iso = (xx + yy + zz) / 3.0
        return (xx, yy, zz, xy, xz, yz, q_iso)
    return None



def parse_dipole(txt: str):
    if not txt: return None
    best=None
    for m in RE_DIP.finditer(txt):
        unit=(m.group(1) or "").lower()
        x,y,z=map(float,m.group(2,3,4))
        if unit=="a.u.": x,y,z = x*AU2D, y*AU2D, z*AU2D
        best = (x,y,z)
        if unit=="debye": break  # prefer explicit Debye value
    if not best: return None
    Dx,Dy,Dz=best
    Mag=(Dx*Dx+Dy*Dy+Dz*Dz)**0.5
    return Dx,Dy,Dz,Mag

# ---------- charge/multiplicity ----------
def parse_charge_mult(txt: str) -> Tuple[Optional[int], Optional[int]]:
    Q=None; M=None
    for pat in [r"Total\s+Charge\s*[:=]\s*(-?\d+)",
                r"Overall\s+charge\s+of\s+the\s+system\s*[:=]\s*(-?\d+)",
                r"Multiplicity\s*[:=]\s*(\d+)"]:
        for m in re.finditer(pat, txt, flags=re.I):
            if "Multiplicity" in pat:
                try: M=int(m.group(1))
                except: pass
            else:
                try: Q=int(m.group(1))
                except: pass
    m = re.search(r"^\s*\*\s*xyz(?:file)?\s+(-?\d+)\s+(\d+)\b.*$", txt, flags=re.I|re.M)
    if m:
        try:
            Q = int(m.group(1))
            M = int(m.group(2))
        except: pass
    return Q,M

def cog(coords):
    return [sum(v[i] for v in coords)/(len(coords)) for i in range(3)]

def cnc(Z, coords):
    Zsum=sum(Z)
    return [sum(Z[k]*coords[k][i] for k in range(len(coords)))/Zsum for i in range(3)]

def geom_sha1(elems, coords, ndp:int=6) -> Optional[str]:
    h=hashlib.sha1()
    for e,(x,y,z) in zip(elems,coords):
        h.update(f"{e}:{round(x,ndp):.6f}:{round(y,ndp):.6f}:{round(z,ndp):.6f};".encode())
    return h.hexdigest()

# ---------- eigenvalues ----------
RE_COLS = re.compile(r"\bNO\b.*\bOCC\b.*E\(Eh\).*E\(eV\)", re.I)
RE_FLOAT = r"[+-]?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?"
RE_ROW   = re.compile(rf"^\s*\d+\s+({RE_FLOAT})\s+({RE_FLOAT})\s+({RE_FLOAT})\s*$")
RE_ROW_T = re.compile(rf"^\s*\d+\s+({RE_FLOAT})\s+({RE_FLOAT})\s+({RE_FLOAT})\s+\S.*$")

def homo_lumo(evals, occs, thr=1e-3):
    if not evals: return None, None
    occ_idx=[i for i,o in enumerate(occs) if o is not None and o>thr]
    virt_idx=[i for i,o in enumerate(occs) if o is not None and o<=thr]
    if not occ_idx:
        return None,(evals[virt_idx[0]] if virt_idx else None)
    h=max(occ_idx); virt_above=[i for i in virt_idx if i>h] or virt_idx
    l=min(virt_above) if virt_above else None
    return evals[h], (evals[l] if l is not None else None)

def parse_eigens(txt: str) -> Optional[Dict[str,Any]]:
    if not txt: return None
    lines=txt.splitlines()
    blocks=[]
    i=0
    while i<len(lines):
        if RE_COLS.search(lines[i]):
            tag='R'
            back=((lines[i-1].upper() if i>=1 else "")+" "+(lines[i-2].upper() if i>=2 else ""))
            if "ALPHA" in back: tag='A'
            if "BETA"  in back: tag='B'
            i+=1
            rows=[]
            while i<len(lines):
                row=lines[i]
                if not row.strip() or not any(ch.isdigit() for ch in row): break
                m=RE_ROW.match(row) or RE_ROW_T.match(row)
                if not m: break
                occ=float(m.group(1)); e_ev=float(m.group(3))
                rows.append((occ,e_ev)); i+=1
            if rows: blocks.append((tag,rows)); continue
        i+=1
    if not blocks: return None
    A=B=R=[]
    for tag,rows in blocks:
        if tag=='A': A=rows
        elif tag=='B': B=rows
        else: R=rows
    res: Dict[str,Any]={}
    if A or B:
        res['spin_case']='UKS'
        a_occ,a_eV=(zip(*A) if A else ([],[]))
        b_occ,b_eV=(zip(*B) if B else ([],[]))
        a_occ,a_eV=list(a_occ),list(a_eV); b_occ,b_eV=list(b_occ),list(b_eV)
        ha,la=homo_lumo(a_eV,a_occ); hb,lb=homo_lumo(b_eV,b_occ)
        res.update({
            'homo_a_eV':ha,'lumo_a_eV':la,'gap_a_eV':(None if ha is None or la is None else la-ha),
            'homo_b_eV':hb,'lumo_b_eV':lb,'gap_b_eV':(None if hb is None or lb is None else lb-hb),
            'homo_eV':max([x for x in [ha,hb] if x is not None], default=None),
            'lumo_eV':min([x for x in [la,lb] if x is not None], default=None),
            'n_orb_a':len(a_eV),'n_orb_b':len(b_eV)
        })
        if res['homo_eV'] is not None and res['lumo_eV'] is not None:
            res['gap_eV']=res['lumo_eV']-res['homo_eV']
    elif R:
        r_occ,r_eV=(zip(*R) if R else ([],[]))
        r_occ,r_eV=list(r_occ),list(r_eV)
        ha,la=homo_lumo(r_eV,r_occ)
        res.update({'spin_case':'RKS','homo_eV':ha,'lumo_eV':la,
                    'gap_eV':(None if ha is None or la is None else la-ha),
                    'n_orb_a':len(r_eV),'n_orb_b':0})
    else:
        return None
    return res


logger = logging.getLogger(__name__)

def setup_logging(level=logging.INFO, log_file_path="sample.log"):
    """Configures the root logger with a console handler and an optional file handler."""
    log_format = (
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    formatter = logging.Formatter(log_format)

    handlers = []
    if log_file_path:
        file_handler = logging.FileHandler(log_file_path, mode='w')
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
        status_msg = f"Logfile: {log_file_path.resolve()}"
    else:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        handlers.append(stream_handler)
        status_msg = "Console only"
    print(status_msg)

    logging.basicConfig(level=level, handlers=handlers)

    logger.info(f"Logger initialized at level: {logging.getLevelName(level)} ({status_msg})")




def get_ranges(lst: list, M: int) -> list[tuple[int, int]]:
    """
    Splits a list of N elements into M parts and returns the (start_index, end_index)
    range for each part. The split is as even as possible.

    Args:
        lst: List
        M: The number of parts (chunks/processors).

    Returns:
        A list of tuples [(start, end), ...]. The end index is exclusive.
    """
    N = len(lst)
    base_count = N // M
    remainder = N % M

    ranges = []
    current_start = 0

    for i in range(M):
        if i < remainder:
            local_n = base_count + 1
        else:
            local_n = base_count

        current_end = current_start + local_n

        ranges.append(lst[current_start:current_end])

        current_start = current_end

    return ranges

class S3DataProcessor:
    """
    Manages the connection to S3, loads the AseDBDataset, and processes
    data entries concurrently using a thread pool.
    """
    def __init__(self, args, rank, size, comm):
        self.args = args
        self.rank = rank
        self.size = size
        self.comm = comm
        self.output_dir = args.output_dir
        self.local_dir = args.local_dir
        self.restart = args.restart
        self.sample_size = args.sample_size
        self.memory_threshold = args.memory_threshold_gb * GiB
        self.chunk_idx = 0
        self.stop_requested = False
        self.is_slurm = 'SLURM_JOB_ID' in os.environ
        
        def handle_signal(signum, frame):
            logger.warning(f"Rank {self.rank} received signal {signum}. Will exit gracefully.")
            self.stop_requested = True
            
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        if hasattr(signal, 'SIGUSR1'):
            signal.signal(signal.SIGUSR1, handle_signal)
            
        self.s3_endpoint_url = "https://s3.echo.stfc.ac.uk"
        self.files_to_process = Path(args.data_source)
        self.group_name = self.files_to_process.stem.replace("_prefix", "").replace("_restart", "")
        self.output_props_final = self.output_dir / f"props_{self.group_name}.parquet"
        self.output_props_rank = self.output_dir / f"props_{self.group_name}_{self.rank}.parquet"
        self.s3 = self.initialize_s3_client(args.login_file)

        if self.files_to_process.name.endswith("_restart.json"):
            self.restart_file = self.files_to_process
            self.restart = True
        else:
            self.restart_file = self.files_to_process.with_name(self.files_to_process.stem + "_restart.json")

        if self.rank == 0:
            logger.info(f"Initializing dataset from: {self.files_to_process.resolve()}")

        with open(self.files_to_process, "r", encoding="utf-8") as f:
            self.data = json_load(f)

        self.prefixes = [x for x in self.data if not self.data[x].get("processed", False)]

        if self.rank == 0:
            logger.info(f"{len(self.prefixes)} files to process out of {len(self.data)}")
            if not self.restart:
                self.output_props_final.unlink(missing_ok=True)
            self.output_dir.mkdir(parents=True, exist_ok=True)

        if self.sample_size < 0:
            self.sample_size = len(self.prefixes)

        max_idx = len(self.prefixes)
        start_idx = self.args.start_index
        end_idx = min(start_idx + self.sample_size, max_idx)

        self.indices_to_process = range(start_idx, end_idx)

        if self.rank == 0:
            logger.info(f"Processing: {self.sample_size} files")
            logger.info(f"Restarts to be written to: {self.restart_file.resolve()}")
            logger.info(f"Dataset size: {max_idx}. Processing indices from {start_idx} to {end_idx} (Total: {len(self.indices_to_process)} samples).")
            logger.info(f"Properties will be written to folder: {self.output_dir.resolve()}")

    def flush_recs(self, recs):
        """Flushes the current batch of records to a part file."""
        if not recs:
            return
        df = pd.DataFrame(recs)
        path = self.output_dir / f"props_{self.group_name}_{self.rank}_part_{self.chunk_idx}.parquet"
        df.to_parquet(path, index=False)
        logger.info(f"Rank {self.rank} flushed {len(recs)} records to {path.name} (Memory: {psutil.Process().memory_info().rss / GiB:.2f} GB)")
        self.chunk_idx += 1

    def initialize_s3_client(self, login_file: Path):
        """Loads credentials and initializes the Boto3 S3 client."""
        with open(login_file, "r") as f:
            credentials = json_load(f)

        config = Config(
            s3={"addressing_style": "path"},
            retries={'max_attempts': 5, 'mode': 'standard'},
            connect_timeout=5,
            read_timeout=30,
            max_pool_connections=20  # Fixed value per rank for internal threading
        )

        return s3_client(
            "s3",
            config=config,
            endpoint_url=self.s3_endpoint_url,
            aws_access_key_id=credentials["access_key"],
            aws_secret_access_key=credentials["secret_key"],
        )

    def process_single(self, idx: int) -> Optional[Tuple[Dict[str, Any], str]]:
        """Processes a single file from S3."""
        x = self.prefixes[idx]
        source = x + self.data[x]['key']
        rec: Dict[str, Any] = {}
        try:
            start_time = time.time()
            rec["argonne_rel"] = x
            rec["data_id"] = source.split("/")[0]

            buffer = BytesIO()
            if self.local_dir:
                local_path = self.local_dir / source
                if not local_path.exists():
                    logger.warning(f"Local file '{local_path}' not found. Skipping.")
                    return None
                buffer.write(local_path.read_bytes())
            else:
                self.s3.download_fileobj(
                    Bucket=self.args.bucket,
                    Key=source,
                    Fileobj=buffer,
                    Config=s3_transfer_options,
                )
            buffer.seek(0)

            decompressor = ZstdDecompressor()
            decompressed_buffer = BytesIO()
            decompressor.copy_stream(buffer, decompressed_buffer)
            decompressed_buffer.seek(0)

            with tar_open(fileobj=decompressed_buffer, mode="r") as tar:
                out = tar.getmember("orca.out")
                inp = tar.getmember("orca.inp")
                prop = tar.getmember("orca.property.txt")

                txt_out = tar.extractfile(out).read().decode('utf-8')
                txt_prop = tar.extractfile(prop).read().decode('utf-8')
                txt_inp = tar.extractfile(inp).read().decode('utf-8')

            dip = parse_dipole(((txt_prop or "") + "\n" + (txt_out or "")))
            if dip:
                Dx, Dy, Dz, Mag = dip
                rec.update({
                    "dipole_x_D": Dx, "dipole_y_D": Dy, "dipole_z_D": Dz, "dipole_mag_D": Mag,
                    "dipole_x_eA": Dx * D2EA, "dipole_y_eA": Dy * D2EA, "dipole_z_eA": Dz * D2EA,
                    "status_dipole": "OK"
                })
            else:
                rec["status_dipole"] = "MISSING"

            quad = parse_quadrupole(txt_out or "")
            if quad:
                xx, yy, zz, xy, xz, yz, q = quad
                rec.update({
                    "Q_xx_B": xx, "Q_yy_B": yy, "Q_zz_B": zz,
                    "Q_xy_B": xy, "Q_xz_B": xz, "Q_yz_B": yz,
                    "Q_isotropic_B": q,
                    "Q_xx_eAA": xx * B2EAA, "Q_yy_eAA": yy * B2EAA, "Q_zz_eAA": zz * B2EAA,
                    "Q_xy_eAA": xy * B2EAA, "Q_xz_eAA": xz * B2EAA, "Q_yz_eAA": yz * B2EAA,
                    "Q_isotropic_eAA": q * B2EAA,
                    "status_quadrupole": "OK"
                })
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
                for k in ["spin_case", "homo_eV", "lumo_eV", "gap_eV",
                          "homo_a_eV", "lumo_a_eV", "gap_a_eV",
                          "homo_b_eV", "lumo_b_eV", "gap_b_eV",
                          "n_orb_a", "n_orb_b"]:
                    if k in eig: rec[k] = eig[k]
            else:
                rec["status_eigs"] = "MISSING"

            rec["process_time_s"] = time.time() - start_time
            return rec, x
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ('404', 'NoSuchKey'):
                logger.warning(f"S3 Key '{source}' not found (404) for index {idx}. Skipping.")
            else:
                logger.error(f"Boto3 error for '{source}' (Index {idx}): {e}")
        except Exception as e:
            logger.error(f"Unexpected error for '{source}' (Index {idx}): {e}")
        return None

    def process_batch(self, batch_indices: list[int]) -> Tuple[list, list]:
        """Processes a batch of indices without threads."""
        recs = []
        processed_prefixes = []
        
        pbar = tqdm(total=len(batch_indices), desc=f"rank {self.rank}",
                    disable=(self.rank != 0 and len(batch_indices) > 100))
        process = psutil.Process()
        
        for idx in batch_indices:
            if getattr(self, 'stop_requested', False):
                break
            try:
                result = self.process_single(idx)
                if result:
                    rec, prefix = result
                    recs.append(rec)
                    processed_prefixes.append(prefix)

                    # Check memory usage
                    if process.memory_info().rss > self.memory_threshold:
                        self.flush_recs(recs)
                        recs = []
            except Exception as e:
                logger.error(f"Error for index {idx}: {e}")
            pbar.update(1)
        pbar.close()

        # Final flush if anything left in recs
        if recs:
            self.flush_recs(recs)
            recs = []

        return recs, processed_prefixes

    def run_mpi(self):
        start_time = time.time()
        if self.rank == 0:
            logger.info(f"Starting execution with {self.size} workers.")

        batches = get_ranges(self.indices_to_process, self.size)
        my_batch = batches[self.rank]
        logger.info(f"rank={self.rank} processing {len(my_batch)} files.")
        
        chunks = get_ranges(my_batch, 10)
        
        if self.comm:
            self.comm.Barrier()
        
        for step, chunk in enumerate(chunks):
            recs, processed_this_rank = self.process_batch(chunk)
            
            # Gather all processed prefixes to Rank 0
            if self.comm:
                all_processed = self.comm.gather(processed_this_rank, root=0)
            else:
                all_processed = [processed_this_rank]

            if self.rank == 0:
                # Flatten list of lists
                flat_processed = [p for sublist in all_processed for p in (sublist or [])]
                if flat_processed:
                    for p in flat_processed:
                        self.data[p]['processed'] = True

                    # Save restart JSON
                    with open(self.restart_file, "w") as f:
                        json_dump(self.data, f, indent=4)
                    logger.info(f"Restart data updated in {self.restart_file.resolve()} (step {step+1}/10)")

        if self.comm:
            self.comm.Barrier()

        if self.rank == 0:
            # Merge Parquet files
            all_dfs = []
            if self.restart and self.output_props_final.exists():
                all_dfs.append(pd.read_parquet(self.output_props_final))

            part_files = sorted(list(self.output_dir.glob(f"props_{self.group_name}_*_part_*.parquet")))
            for rank_part_file in part_files:
                try:
                    all_dfs.append(pd.read_parquet(rank_part_file))
                    rank_part_file.unlink(missing_ok=True) # Clean up
                except Exception as e:
                    logger.warning(f"Failed to process or unlink part file {rank_part_file}: {e}")

            end_time = time.time()
            elapsed_time = end_time - start_time

            if all_dfs:
                final_df = pd.concat(all_dfs, ignore_index=True)
                final_df.to_parquet(self.output_props_final, index=False)
                logger.info(f"Merged {len(all_dfs)} parts into {self.output_props_final.resolve()}")
                
                if "process_time_s" in final_df.columns:
                    times = final_df["process_time_s"]
                    n = len(final_df)
                    logger.info("-" * 30)
                    logger.info("Processing Statistics:")
                    logger.info(f"  Files processed: {n}")
                    logger.info(f"  Total time:      {times.sum():.2f} s")
                    logger.info(f"  Mean time:       {times.mean():.4f} s/file")
                    logger.info(f"  Min time:        {times.min():.4f} s/file")
                    logger.info(f"  Max time:        {times.max():.4f} s/file")
                    logger.info(f"  Throughput:      {n / elapsed_time:.2f} files/s (aggregated)")
                    logger.info("-" * 30)
            else:
                raise RuntimeError(f"No data was successfully processed; {self.output_props_final} was not created.")

            logger.info("-" * 50)
            logger.info("Processing complete.")
            logger.info(f"Total files attempted: {len(self.indices_to_process)}")
            logger.info(f"Total execution time: {elapsed_time:.2f} seconds")
