"""
Standalone utility for cross-referencing Parquet files with their corresponding ExtXYZ files.
Verifies that all properties stored in the Parquet dataset perfectly match the data embedded
in `atoms.info`.
"""

import argparse
from collections import defaultdict
import json
import logging
import math
from pathlib import Path

from ase.io import iread
import numpy as np
import pandas as pd
from tqdm import tqdm

from .utils import setup_logging


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
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
        "--json",
        "--json-file",
        "--data-source",
        "--prefix-json",
        dest="json_file",
        type=Path,
        default=None,
        help="Optional path to prefix JSON file (e.g. ani2x_prefix.json) to cross-reference keys with Parquet and ExtXYZ.",
    )
    parser.add_argument(
        "--skip-keys",
        nargs="*",
        default=["process_time_s", "argonne_rel", "data_id"],
        help="Properties to skip during exact matching.",
    )
    return parser.parse_args()


def _to_clean_array(val):
    """Normalize array-like or sequence object into a multi-dimensional numpy array."""
    if isinstance(val, np.ndarray) and np.issubdtype(val.dtype, np.number):
        return val
    return np.array(val.tolist() if hasattr(val, "tolist") else val)


def main() -> None:
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
    if args.json_file is not None and not args.json_file.exists():
        raise FileNotFoundError(f"JSON file '{args.json_file}' does not exist.")

    logger.info(f"Loading Parquet file from {args.parquet}...")
    df = pd.read_parquet(args.parquet)
    if "geom_sha1" not in df.columns:
        raise KeyError(
            "'geom_sha1' key is missing from Parquet file. Cannot perform alignment."
        )

    logger.info(f"Loaded {len(df)} records from Parquet.")
    parquet_by_sha = {}
    parquet_by_argone_rel = {}
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Indexing Parquet records", disable=len(df) == 0):
        sha = row["geom_sha1"]
        parquet_by_sha[sha] = row
        rel = row.get("argonne_rel")
        if rel is not None and pd.notna(rel):
            parquet_by_argone_rel[rel] = row

    logger.info(f"Loading ExtXYZ file from {args.extxyz} (this may take a moment)...")
    all_atoms = list(tqdm(iread(str(args.extxyz), index=":"), total=len(df), desc="Loading ExtXYZ"))
    logger.info(f"Loaded {len(all_atoms)} structures from ExtXYZ.")

    xyz_by_sha_list = defaultdict(list)
    xyz_by_argone_rel_list = defaultdict(list)
    for i, at in enumerate(tqdm(all_atoms, desc="Indexing ExtXYZ structures", disable=len(all_atoms) == 0)):
        sha = at.info.get("geom_sha1")
        rel = at.info.get("argonne_rel")
        if rel is not None and not (isinstance(rel, float) and math.isnan(rel)):
            xyz_by_argone_rel_list[rel].append(at)
        else:
            logger.warning(f"ExtXYZ frame {i} is missing 'argonne_rel' in atoms.info.")
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

    if args.json_file is not None:
        logger.info(f"\n--- Alignment with JSON ({args.json_file.name}) ---")
        with open(args.json_file, "r", encoding="utf-8") as f:
            json_data = json.load(f)

        if isinstance(json_data, dict):
            json_keys = set(json_data.keys())
            processed_count = sum(
                1
                for v in json_data.values()
                if isinstance(v, dict) and v.get("processed", False)
            )
            logger.info(f"Loaded {len(json_keys)} keys from {args.json_file.name}.")
            if processed_count > 0:
                logger.info(
                    f"   JSON metadata: {processed_count}/{len(json_keys)} keys marked as processed."
                )
        elif isinstance(json_data, list):
            json_keys = set(json_data)
            logger.info(f"Loaded {len(json_keys)} keys from {args.json_file.name}.")
        else:
            raise ValueError(
                f"Expected dict or list in '{args.json_file}', got {type(json_data).__name__}."
            )

        missing_in_pq_from_json = json_keys - pq_argonne_rels
        extra_in_pq_from_json = pq_argonne_rels - json_keys
        missing_in_xyz_from_json = json_keys - xyz_argonne_rels
        extra_in_xyz_from_json = xyz_argonne_rels - json_keys

        json_keys_norm = {k.rstrip("/") for k in json_keys}
        pq_keys_norm = {k.rstrip("/") for k in pq_argonne_rels}
        xyz_keys_norm = {k.rstrip("/") for k in xyz_argonne_rels}

        pq_match = False
        if not missing_in_pq_from_json and not extra_in_pq_from_json:
            logger.info(f"✅ All {len(json_keys)} keys in JSON match Parquet records.")
            pq_match = True
        elif json_keys_norm == pq_keys_norm:
            logger.warning(
                f"⚠️ Parquet records match JSON keys when ignoring trailing slashes ({len(json_keys)} keys)."
            )
            pq_match = True
        else:
            if missing_in_pq_from_json:
                logger.error(
                    f"❌ {len(missing_in_pq_from_json)} keys found in JSON but missing from Parquet."
                )
                sample = sorted(list(missing_in_pq_from_json))[:5]
                logger.error(f"   Examples missing in Parquet: {sample}")
            if extra_in_pq_from_json:
                logger.error(
                    f"❌ {len(extra_in_pq_from_json)} records found in Parquet but missing from JSON."
                )
                sample = sorted(list(extra_in_pq_from_json))[:5]
                logger.error(f"   Examples extra in Parquet: {sample}")

        xyz_match = False
        if not missing_in_xyz_from_json and not extra_in_xyz_from_json:
            logger.info(f"✅ All {len(json_keys)} keys in JSON match ExtXYZ structures.")
            xyz_match = True
        elif json_keys_norm == xyz_keys_norm:
            logger.warning(
                f"⚠️ ExtXYZ structures match JSON keys when ignoring trailing slashes ({len(json_keys)} keys)."
            )
            xyz_match = True
        else:
            if missing_in_xyz_from_json:
                logger.error(
                    f"❌ {len(missing_in_xyz_from_json)} keys found in JSON but missing from ExtXYZ."
                )
                sample = sorted(list(missing_in_xyz_from_json))[:5]
                logger.error(f"   Examples missing in ExtXYZ: {sample}")
            if extra_in_xyz_from_json:
                logger.error(
                    f"❌ {len(extra_in_xyz_from_json)} structures found in ExtXYZ but missing from JSON."
                )
                sample = sorted(list(extra_in_xyz_from_json))[:5]
                logger.error(f"   Examples extra in ExtXYZ: {sample}")

        if pq_match and xyz_match:
            logger.info(
                f"✅ JSON Alignment perfect: All {len(json_keys)} keys correspond across JSON, Parquet, and ExtXYZ."
            )
        else:
            logger.error(
                "❌ JSON Alignment failed: Key discrepancies detected between JSON, Parquet, and ExtXYZ."
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

    for argonne_rel in tqdm(
        sorted(common_argonne_rels),
        desc="Validating properties",
        disable=len(common_argonne_rels) == 0,
    ):
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

            # Unwrap 0-d arrays if any
            if isinstance(pq_val, np.ndarray) and pq_val.ndim == 0:
                pq_val = pq_val.item()
            if isinstance(xyz_val, np.ndarray) and xyz_val.ndim == 0:
                xyz_val = xyz_val.item()

            is_pq_seq = isinstance(pq_val, (np.ndarray, list, tuple, pd.Series))
            is_xyz_seq = isinstance(xyz_val, (np.ndarray, list, tuple, pd.Series))

            if is_pq_seq or is_xyz_seq:
                if not (is_pq_seq and is_xyz_seq):
                    logger.error(
                        f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                        f"Sequence type mismatch: Parquet={type(pq_val).__name__} != XYZ={type(xyz_val).__name__}"
                    )
                    mismatches += 1
                else:
                    pq_arr = _to_clean_array(pq_val)
                    xyz_arr = _to_clean_array(xyz_val)
                    if pq_arr.shape != xyz_arr.shape:
                        logger.error(
                            f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                            f"Array shape mismatch: Parquet shape {pq_arr.shape} != XYZ shape {xyz_arr.shape}"
                        )
                        mismatches += 1
                    elif np.issubdtype(pq_arr.dtype, np.number) and np.issubdtype(xyz_arr.dtype, np.number):
                        if not np.allclose(pq_arr, xyz_arr, rtol=1e-9, atol=1e-12, equal_nan=True):
                            logger.error(
                                f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                                f"NDArray mismatch: Parquet={pq_val!r} != XYZ={xyz_val!r}"
                            )
                            mismatches += 1
                    else:
                        if not np.array_equal(pq_arr, xyz_arr):
                            logger.error(
                                f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                                f"List/Array mismatch: Parquet={pq_val!r} != XYZ={xyz_val!r}"
                            )
                            mismatches += 1
            else:
                pq_is_nan = pq_val is None or (
                    isinstance(pq_val, (float, np.floating)) and math.isnan(pq_val)
                ) or (isinstance(pq_val, type(pd.NA)) and pq_val is pd.NA)
                xyz_is_nan = xyz_val is None or (
                    isinstance(xyz_val, (float, np.floating)) and math.isnan(xyz_val)
                ) or (isinstance(xyz_val, type(pd.NA)) and xyz_val is pd.NA)

                if pq_is_nan or xyz_is_nan:
                    if not (pq_is_nan and xyz_is_nan):
                        logger.error(
                            f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: Parquet is NaN/None but XYZ has {xyz_val!r}"
                            if pq_is_nan
                            else f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: XYZ is NaN/None but Parquet has {pq_val!r}"
                        )
                        mismatches += 1
                elif isinstance(pq_val, (bool, np.bool_)) or isinstance(xyz_val, (bool, np.bool_)):
                    if not (isinstance(pq_val, (bool, np.bool_)) and isinstance(xyz_val, (bool, np.bool_))) or bool(pq_val) != bool(xyz_val):
                        logger.error(
                            f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: Parquet={pq_val!r} != XYZ={xyz_val!r}"
                        )
                        mismatches += 1
                elif isinstance(pq_val, (int, float, np.number)) and isinstance(xyz_val, (int, float, np.number)):
                    if isinstance(pq_val, (float, np.floating)) or isinstance(xyz_val, (float, np.floating)):
                        if not math.isclose(float(pq_val), float(xyz_val), rel_tol=1e-9, abs_tol=1e-12):
                            logger.error(
                                f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: Parquet={pq_val} != XYZ={xyz_val} (diff={abs(float(pq_val) - float(xyz_val))})"
                            )
                            mismatches += 1
                    else:
                        if pq_val != xyz_val:
                            logger.error(
                                f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: Parquet={pq_val!r} != XYZ={xyz_val!r}"
                            )
                            mismatches += 1
                else:
                    if pq_val != xyz_val:
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
