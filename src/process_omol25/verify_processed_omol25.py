"""
Standalone utility for cross-referencing Parquet files with their corresponding ExtXYZ files.
Verifies that all properties stored in the Parquet dataset perfectly match the data embedded
in `atoms.info`.
"""

import argparse
import math
import logging
import json
from pathlib import Path
from collections import defaultdict

import pandas as pd
import numpy as np
from ase.io import read

try:
    from .process_omol25 import setup_logging
except ImportError:
    # Fallback for if it's run as a standalone script outside the package
    def setup_logging(level=logging.INFO, **kwargs):
        """
        Setup basic logging configuration for standalone execution.

        Parameters
        ----------
        level : int, optional
            The logging level to use, by default logging.INFO.
        **kwargs : dict
            Additional keyword arguments for basicConfig.
        """
        logging.basicConfig(
            level=level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )


logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments for the verification script.

    Returns
    -------
    argparse.Namespace
        The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Verify consistency between a Parquet dataset and its corresponding ExtXYZ file."
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        required=True,
        help="Path to the .parquet file to check.",
    )
    parser.add_argument(
        "--extxyz",
        type=Path,
        required=True,
        help="Path to the corresponding .xyz file to cross-reference.",
    )
    parser.add_argument(
        "--skip-keys",
        nargs="*",
        default=["process_time_s", "argonne_rel", "data_id"],
        help="Properties to skip during exact matching.",
    )
    return parser.parse_args()


def main():
    """
    Main entry point for verifying Parquet and ExtXYZ consistency.

    This function coordinates the loading of both files, structural alignment,
    and property validation.
    """
    args = parse_args()
    setup_logging()

    if not args.parquet.exists():
        raise FileNotFoundError(f"Parquet file '{args.parquet}' does not exist.")
    if not args.extxyz.exists():
        raise FileNotFoundError(f"ExtXYZ file '{args.extxyz}' does not exist.")

    logger.info(f"Loading Parquet file from {args.parquet}...")
    df = pd.read_parquet(args.parquet)
    if "geom_sha1" not in df.columns:
        raise KeyError(
            "'geom_sha1' key is missing from Parquet file. Cannot perform alignment."
        )

    logger.info(f"Loaded {len(df)} records from Parquet.")
    parquet_by_sha = {row["geom_sha1"]: row for _, row in df.iterrows()}
    parquet_by_argone_rel = {row["argonne_rel"]: row for _, row in df.iterrows()}
    logger.info(f"Loading ExtXYZ file from {args.extxyz} (this may take a moment)...")
    all_atoms = read(str(args.extxyz), index=":")
    if not isinstance(all_atoms, list):
        all_atoms = [all_atoms] if all_atoms else []
    logger.info(f"Loaded {len(all_atoms)} structures from ExtXYZ.")

    xyz_by_sha_list = defaultdict(list)
    xyz_by_argone_rel_list = defaultdict(list)
    for i, at in enumerate(all_atoms):
        sha = at.info.get("geom_sha1")
        rel = at.info.get("argonne_rel")
        xyz_by_argone_rel_list[rel].append(at)
        if not sha:
            logger.warning(f"ExtXYZ frame {i} is missing 'geom_sha1' in atoms.info.")
            continue
        xyz_by_sha_list[sha].append(at)

    xyz_by_sha = {s: atoms[-1] for s, atoms in xyz_by_sha_list.items()}
    xyz_by_argone_rel = {r: atoms[-1] for r, atoms in xyz_by_argone_rel_list.items()}

    if len(xyz_by_sha) != len(xyz_by_argone_rel):
        logger.warning(
            f"❌ Structural redundancy detected: {len(xyz_by_sha)} unique SHAs vs {len(xyz_by_argone_rel)} unique argonne_rels."
        )

        def get_dump_entry(at):
            info = dict(at.info)
            rel = info.get("argonne_rel")
            pq_row = parquet_by_argone_rel.get(rel)
            pq_data = pq_row.to_dict() if pq_row is not None else None
            return {"xyz": info, "parquet": pq_data}

        duplicates = {
            "by_sha": {
                s: [get_dump_entry(at) for at in atoms]
                for s, atoms in xyz_by_sha_list.items()
                if len(atoms) > 1
            },
            "by_argonne_rel": {
                r: [get_dump_entry(at) for at in atoms]
                for r, atoms in xyz_by_argone_rel_list.items()
                if len(atoms) > 1
            },
        }

        dup_file = f"duplicates_{args.parquet.stem}.json"
        with open(dup_file, "w") as f:
            json.dump(duplicates, f, indent=4, default=str)
        logger.info(f"Dumped duplicates to '{dup_file}'.")

    pq_keys = set(parquet_by_sha.keys())
    xyz_keys = set(xyz_by_sha.keys())
    pq_argonne_rels = set(parquet_by_argone_rel.keys())
    xyz_argonne_rels = set(xyz_by_argone_rel.keys())

    missing_in_xyz = pq_keys - xyz_keys
    missing_in_pq = xyz_keys - pq_keys
    missing_in_xyz_argonne_rel = pq_argonne_rels - xyz_argonne_rels
    missing_in_pq_argonne_rel = xyz_argonne_rels - pq_argonne_rels

    logger.info("\n--- Structural Alignment by sha---")
    if missing_in_xyz:
        logger.error(
            f"❌ {len(missing_in_xyz)} structures found in Parquet but missing from ExtXYZ."
        )
    if missing_in_pq:
        logger.error(
            f"❌ {len(missing_in_pq)} structures found in ExtXYZ but missing from Parquet."
        )

    if not missing_in_xyz and not missing_in_pq:
        logger.info(
            f"✅ Structural alignment perfect: both files contain exactly {len(pq_keys)} uniquely matched molecules."
        )

    logger.info("\n--- Structural Alignment by argonne_rel ---")
    if missing_in_xyz_argonne_rel:
        logger.error(
            f"❌ {len(missing_in_xyz_argonne_rel)} structures found in Parquet but missing from ExtXYZ."
        )
    if missing_in_pq_argonne_rel:
        logger.error(
            f"❌ {len(missing_in_pq_argonne_rel)} structures found in ExtXYZ but missing from Parquet."
        )

    if not missing_in_xyz_argonne_rel and not missing_in_pq_argonne_rel:
        logger.info(
            f"✅ Structural alignment perfect: both files contain exactly {len(pq_argonne_rels)} uniquely matched molecules."
        )

    logger.info("\n--- Property Validation by argonne_rel ---")
    common_argonne_rels = pq_argonne_rels.intersection(xyz_argonne_rels)
    skip_keys = set(args.skip_keys)
    mismatches = 0
    checked_props = {
        key: 0
        for key in df.columns
        if key not in skip_keys
        and key not in ("argonne_rel", "process_time_s")
        and "time" not in key.lower()
    }

    for argonne_rel in common_argonne_rels:
        row = parquet_by_argone_rel[argonne_rel]
        info = xyz_by_argone_rel[argonne_rel].info

        for key, pq_val in row.items():
            if (
                key in skip_keys
                or key in ("argonne_rel", "process_time_s")
                or "time" in key.lower()
            ):
                continue

            if key not in info:
                logger.error(
                    f"  [Mismatch] argonne_rel={argonne_rel}: Key '{key}' missing from XYZ `atoms.info`."
                )
                mismatches += 1
                continue

            xyz_val = info[key]

            pq_is_nan = pq_val is None or (
                isinstance(pq_val, float) and math.isnan(pq_val)
            )
            xyz_is_nan = xyz_val is None or (
                isinstance(xyz_val, float) and math.isnan(xyz_val)
            )

            if pq_is_nan:
                if not xyz_is_nan:
                    logger.error(
                        f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: Parquet is NaN/None but XYZ has {xyz_val!r}"
                    )
                    mismatches += 1
            else:
                if isinstance(pq_val, float) and isinstance(xyz_val, float):
                    if not math.isclose(pq_val, xyz_val, rel_tol=1e-9, abs_tol=1e-12):
                        logger.error(
                            f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: Parquet={pq_val} != XYZ={xyz_val} (diff={abs(pq_val - xyz_val)})"
                        )
                        mismatches += 1
                elif pq_val != xyz_val:
                    try:
                        if isinstance(pq_val, np.ndarray) or isinstance(
                            xyz_val, np.ndarray
                        ):
                            if not np.allclose(pq_val, xyz_val, equal_nan=True):
                                logger.error(
                                    f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: NDArray mismatch"
                                )
                                mismatches += 1
                            continue
                        elif isinstance(pq_val, list) or isinstance(xyz_val, list):
                            if not np.allclose(
                                np.array(pq_val), np.array(xyz_val), equal_nan=True
                            ):
                                logger.error(
                                    f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: List/Array mismatch"
                                )
                                mismatches += 1
                            continue
                    except ImportError:
                        pass

                    logger.error(
                        f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: Parquet={pq_val!r} != XYZ={xyz_val!r}"
                    )
                    mismatches += 1

            if key in checked_props:
                checked_props[key] += 1

    if mismatches == 0:
        logger.info(
            f"\n✅ Data Consistency Verified! 0 mismatches found across {len(common_argonne_rels)} shared structures."
        )
    else:
        logger.error(
            f"\n❌ Verification Failed. Encountered {mismatches} property inconsistencies."
        )


if __name__ == "__main__":
    main()
