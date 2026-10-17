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
from typing import Optional

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


def _is_null_or_nan(val) -> bool:
    """Check if a value is None, NaN, pd.NA, or empty null representation."""
    if val is None:
        return True
    if isinstance(val, np.ndarray):
        if val.ndim == 0:
            item = val.item()
            if item is None:
                return True
            if isinstance(item, (float, np.floating)) and math.isnan(item):
                return True
            return False
        return val.size == 0
    if isinstance(val, (list, tuple)):
        return len(val) == 0
    if isinstance(val, type(pd.NA)) and val is pd.NA:
        return True
    if isinstance(val, (float, np.floating)) and math.isnan(val):
        return True
    return False


def _format_val_summary(val) -> str:
    """Format a concise summary of a property value (type, shape, and preview)."""
    if _is_null_or_nan(val):
        return "NaN/None"
    if isinstance(val, np.ndarray):
        if val.ndim == 0:
            return f"array({val.item()!r}, dtype={val.dtype})"
        return f"array(shape={val.shape}, dtype={val.dtype})"
    if isinstance(val, (list, tuple)):
        if val and isinstance(val[0], (list, tuple)):
            dims = f"{len(val)}x{len(val[0])}"
            return f"list(shape={dims})"
        if len(val) > 4:
            return f"list(len={len(val)}, preview={val[:3]}...)"
        return f"list({val!r})"
    if isinstance(val, (int, float, np.number, bool, np.bool_)):
        return f"{type(val).__name__}({val})"
    s = repr(val)
    if len(s) > 50:
        return f"{type(val).__name__}({s[:47]}...)"
    return f"{type(val).__name__}({s})"


def get_structure_charge(at, pq_row=None) -> Optional[float]:
    """
    Extract total charge of a structure from atoms.info, initial charges, or Parquet row.

    Parameters
    ----------
    at : ase.Atoms or None
        The atomic structure.
    pq_row : pd.Series or dict or None
        The corresponding Parquet row.

    Returns
    -------
    Optional[float]
        The total charge if found, else None.
    """
    if at is not None and hasattr(at, "info"):
        for key in ("total_charge_e", "charge", "total_charge"):
            if key in at.info:
                val = at.info[key]
                if val is not None and not (isinstance(val, float) and math.isnan(val)):
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        pass
        try:
            init_charges = at.get_initial_charges()
            if init_charges is not None and len(init_charges) > 0:
                return float(np.sum(init_charges))
        except Exception:
            pass

    if pq_row is not None:
        for key in ("total_charge_e", "charge", "total_charge"):
            if key in pq_row:
                val = pq_row[key]
                if pd.notna(val):
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        pass
    return None


def main() -> None:
    """
    Main entry point for verifying Parquet and ExtXYZ consistency.

    This function coordinates the loading of both files, structural alignment,
    and property validation.
    """
    args = parse_args()
    setup_logging()

    if args.json_file is None:
        stem = args.parquet.stem
        raw_prefix = stem[6:] if stem.startswith("props_") else stem
        candidate_names = [
            f"{stem}_prefix_restart.json",
            f"{raw_prefix}_prefix_restart.json",
            f"{stem}_restart.json",
            f"{raw_prefix}_restart.json",
            f"{stem}_prefix.json",
            f"{raw_prefix}_prefix.json",
            f"{stem}.json",
            f"{raw_prefix}.json",
        ]
        search_dirs = [args.parquet.parent]
        cwd = Path.cwd()
        if cwd not in search_dirs:
            search_dirs.append(cwd)
        for cand_dir in search_dirs:
            for name in candidate_names:
                cand = cand_dir / name
                if cand.exists() and cand.is_file():
                    args.json_file = cand
                    logger.info(f"Auto-detected prefix JSON file: '{args.json_file}'")
                    break
            if args.json_file is not None:
                break

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
    parquet_by_sha_list = defaultdict(list)
    parquet_by_argone_rel_list = defaultdict(list)
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Indexing Parquet records", disable=len(df) == 0):
        sha = row["geom_sha1"]
        parquet_by_sha_list[sha].append(row)
        rel = row.get("argonne_rel")
        if rel is not None and pd.notna(rel):
            parquet_by_argone_rel_list[rel].append(row)

    parquet_by_sha = {s: rows[-1] for s, rows in parquet_by_sha_list.items()}
    parquet_by_argone_rel = {r: rows[-1] for r, rows in parquet_by_argone_rel_list.items()}

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

    confirmed_duplicates_by_sha = defaultdict(list)
    distinct_states_by_sha = defaultdict(list)

    for s, atoms in xyz_by_sha_list.items():
        if len(atoms) <= 1:
            continue

        # Group structures sharing geom_sha1 by (atomic species, total charge)
        state_groups = defaultdict(list)
        for at in atoms:
            rel = at.info.get("argonne_rel")
            species = tuple(at.get_chemical_symbols())
            charge = get_structure_charge(at, parquet_by_argone_rel.get(rel))
            state_groups[(species, charge)].append(at)

        for (species, charge), grp in state_groups.items():
            if len(grp) > 1:
                # Same SHA, same species, and same charge -> confirmed duplicates!
                confirmed_duplicates_by_sha[s].extend(grp)
            else:
                # Same SHA, but distinct species or distinct charge -> not duplicates
                distinct_states_by_sha[s].extend(grp)

    # Check for duplicate frames within the same argonne_rel
    xyz_dups_by_rel = {
        r: atoms for r, atoms in xyz_by_argone_rel_list.items() if len(atoms) > 1
    }

    if confirmed_duplicates_by_sha:
        logger.warning(
            f"❌ Confirmed duplicate structures detected: {len(confirmed_duplicates_by_sha)} unique SHAs contain "
            f"duplicate structures with identical species and charge."
        )
        for s, dups in list(confirmed_duplicates_by_sha.items())[:5]:
            rels = [d.info.get("argonne_rel") for d in dups]
            sp = list(dups[0].get_chemical_symbols())
            ch = get_structure_charge(dups[0], parquet_by_argone_rel.get(rels[0]))
            logger.warning(
                f"   SHA {s}: {len(dups)} duplicate structures (species={sp}, charge={ch}) across argonne_rels: {rels}"
            )
        if len(confirmed_duplicates_by_sha) > 5:
            logger.warning(
                f"   ... and {len(confirmed_duplicates_by_sha) - 5} more duplicate SHAs."
            )

    if distinct_states_by_sha:
        logger.info(
            f"ℹ️ {len(distinct_states_by_sha)} unique SHAs shared by distinct states "
            f"(differing in atomic species or charge; not duplicates)."
        )
        for s, states in list(distinct_states_by_sha.items())[:5]:
            details = [
                f"(argonne_rel={at.info.get('argonne_rel')}, species={''.join(at.get_chemical_symbols())}, charge={get_structure_charge(at, parquet_by_argone_rel.get(at.info.get('argonne_rel')))})"
                for at in states
            ]
            logger.info(f"   SHA {s} states: {', '.join(details)}")
        if len(distinct_states_by_sha) > 5:
            logger.info(
                f"   ... and {len(distinct_states_by_sha) - 5} more multi-state SHAs."
            )

    if not confirmed_duplicates_by_sha and not distinct_states_by_sha:
        logger.info(f"✅ All {len(xyz_by_sha)} structures have unique SHAs (0 duplicates detected).")

    if confirmed_duplicates_by_sha or xyz_dups_by_rel:
        logger.warning(
            f"❌ Structural redundancy detected: {len(xyz_by_sha)} unique SHAs vs {len(xyz_by_argone_rel)} unique argonne_rels "
            f"({len(confirmed_duplicates_by_sha)} confirmed duplicate SHAs, {len(xyz_dups_by_rel)} duplicate argonne_rels)."
        )
    elif len(xyz_by_sha) != len(xyz_by_argone_rel):
        logger.info(
            f"ℹ️ Structural alignment note: {len(xyz_by_sha)} unique SHAs vs {len(xyz_by_argone_rel)} unique argonne_rels "
            f"(accounted for by {len(distinct_states_by_sha)} multi-state geometries with differing species or charge)."
        )

    if confirmed_duplicates_by_sha or xyz_dups_by_rel or distinct_states_by_sha:
        def get_dump_entry(at):
            info = dict(at.info)
            rel = info.get("argonne_rel")
            pq_row = parquet_by_argone_rel.get(rel)
            pq_data = pq_row.to_dict() if pq_row is not None else None
            return {
                "argonne_rel": rel,
                "species": list(at.get_chemical_symbols()),
                "charge": get_structure_charge(at, pq_row),
                "xyz": info,
                "parquet": pq_data,
            }

        duplicates = {
            "confirmed_duplicates": {
                s: [get_dump_entry(at) for at in atoms]
                for s, atoms in confirmed_duplicates_by_sha.items()
            },
            "distinct_states_by_sha": {
                s: [get_dump_entry(at) for at in atoms]
                for s, atoms in distinct_states_by_sha.items()
            },
            "by_sha": {
                s: [get_dump_entry(at) for at in atoms]
                for s, atoms in confirmed_duplicates_by_sha.items()
            },
            "by_argonne_rel": {
                r: [get_dump_entry(at) for at in atoms]
                for r, atoms in xyz_dups_by_rel.items()
            },
        }

        dup_file = args.parquet.parent / f"duplicates_{args.parquet.stem}.json"
        with open(dup_file, "w") as f:
            json.dump(duplicates, f, indent=4, default=str)
        if confirmed_duplicates_by_sha or xyz_dups_by_rel:
            logger.info(f"Dumped duplicates to '{dup_file}'.")
        else:
            logger.info(f"Dumped SHA state breakdown to '{dup_file}'.")

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

    restart_status_by_rel = {}
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
            for k, v in json_data.items():
                if isinstance(v, dict):
                    p = v.get("processed")
                    status_str = f"processed={p}" if p is not None else "in_json"
                else:
                    status_str = "in_json"
                restart_status_by_rel[k] = status_str
                restart_status_by_rel[k.rstrip("/")] = status_str
                restart_status_by_rel[k.rstrip("/") + "/"] = status_str
            logger.info(f"Loaded {len(json_keys)} keys from {args.json_file.name}.")
            if processed_count > 0:
                logger.info(
                    f"   JSON metadata: {processed_count}/{len(json_keys)} keys marked as processed."
                )
        elif isinstance(json_data, list):
            json_keys = set(json_data)
            for k in json_data:
                restart_status_by_rel[k] = "in_json"
                restart_status_by_rel[k.rstrip("/")] = "in_json"
                restart_status_by_rel[k.rstrip("/") + "/"] = "in_json"
            logger.info(f"Loaded {len(json_keys)} keys from {args.json_file.name}.")
        else:
            raise ValueError(
                f"Expected dict or list in '{args.json_file}', got {type(json_data).__name__}."
            )

        num_json = len(json_keys)
        num_pq = len(pq_argonne_rels)
        num_xyz = len(xyz_argonne_rels)

        if num_json > num_pq and num_json > num_xyz:
            logger.warning(
                f"⚠️ JSON file contains more keys than Parquet and ExtXYZ: "
                f"{num_json} in JSON vs {num_pq} in Parquet and {num_xyz} in ExtXYZ "
                f"(surplus: {num_json - num_pq} vs Parquet, {num_json - num_xyz} vs ExtXYZ)."
            )
        if num_json > num_pq:
            logger.warning(
                f"⚠️ JSON file contains more keys than Parquet records: "
                f"{num_json} JSON keys vs {num_pq} Parquet records "
                f"({num_json - num_pq} surplus keys in JSON)."
            )
        elif num_json < num_pq:
            logger.warning(
                f"⚠️ Parquet records contain more keys than JSON: "
                f"{num_pq} Parquet records vs {num_json} JSON keys "
                f"({num_pq - num_json} surplus records in Parquet)."
            )

        if num_json > num_xyz:
            logger.warning(
                f"⚠️ JSON file contains more keys than ExtXYZ structures: "
                f"{num_json} JSON keys vs {num_xyz} ExtXYZ structures "
                f"({num_json - num_xyz} surplus keys in JSON)."
            )
        elif num_json < num_xyz:
            logger.warning(
                f"⚠️ ExtXYZ structures contain more keys than JSON: "
                f"{num_xyz} ExtXYZ structures vs {num_json} JSON keys "
                f"({num_xyz - num_json} surplus structures in ExtXYZ)."
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
    mismatches_parquet_has_data_xyz_missing = 0
    mismatches_xyz_has_data_parquet_missing = 0
    mismatches_value_diff = 0
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

        restart_tag = ""
        if args.json_file is not None:
            status = restart_status_by_rel.get(argonne_rel)
            if status is not None:
                restart_tag = f" [Restart status: {status}]"
            else:
                restart_tag = " [Restart status: not_in_json]"

        all_keys = set(row.keys()) | set(info.keys())

        for key in sorted(all_keys):
            if (
                key in skip_keys
                or key in ("argonne_rel", "process_time_s")
                or "time" in key.lower()
            ):
                continue

            in_pq = key in row
            pq_val = row[key] if in_pq else None
            pq_is_null = (not in_pq) or _is_null_or_nan(pq_val)

            in_xyz = key in info
            xyz_val = info[key] if in_xyz else None
            xyz_is_null = (not in_xyz) or _is_null_or_nan(xyz_val)

            # 1. Both are null/absent: consistent
            if pq_is_null and xyz_is_null:
                continue

            # 2. Data is present in Parquet, but missing/null in ExtXYZ
            if not pq_is_null and xyz_is_null:
                pq_summary = _format_val_summary(pq_val)
                if not in_xyz:
                    logger.error(
                        f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                        f"Data IS PRESENT in Parquet ({pq_summary}) but key is MISSING from ExtXYZ `atoms.info`.{restart_tag}"
                    )
                else:
                    logger.error(
                        f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                        f"Data IS PRESENT in Parquet ({pq_summary}) but ExtXYZ `atoms.info` is NaN/None.{restart_tag}"
                    )
                mismatches += 1
                mismatches_parquet_has_data_xyz_missing += 1
                continue

            # 3. Data is present in ExtXYZ, but missing/null in Parquet
            if pq_is_null and not xyz_is_null:
                xyz_summary = _format_val_summary(xyz_val)
                if not in_pq:
                    logger.error(
                        f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                        f"Data IS PRESENT in ExtXYZ `atoms.info` ({xyz_summary}) but key is MISSING from Parquet columns.{restart_tag}"
                    )
                else:
                    logger.error(
                        f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                        f"Data IS PRESENT in ExtXYZ `atoms.info` ({xyz_summary}) but Parquet is NaN/None.{restart_tag}"
                    )
                mismatches += 1
                mismatches_xyz_has_data_parquet_missing += 1
                continue

            # 4. Both have non-null data: compare values
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
                        f"Sequence type mismatch: Parquet={type(pq_val).__name__} != XYZ={type(xyz_val).__name__}.{restart_tag}"
                    )
                    mismatches += 1
                    mismatches_value_diff += 1
                else:
                    pq_arr = _to_clean_array(pq_val)
                    xyz_arr = _to_clean_array(xyz_val)
                    if pq_arr.shape != xyz_arr.shape:
                        logger.error(
                            f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                            f"Array shape mismatch: Parquet shape {pq_arr.shape} != XYZ shape {xyz_arr.shape}.{restart_tag}"
                        )
                        mismatches += 1
                        mismatches_value_diff += 1
                    elif np.issubdtype(pq_arr.dtype, np.number) and np.issubdtype(xyz_arr.dtype, np.number):
                        if not np.allclose(pq_arr, xyz_arr, rtol=1e-9, atol=1e-12, equal_nan=True):
                            logger.error(
                                f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                                f"NDArray mismatch: Parquet={_format_val_summary(pq_val)} != XYZ={_format_val_summary(xyz_val)}.{restart_tag}"
                            )
                            mismatches += 1
                            mismatches_value_diff += 1
                    else:
                        if not np.array_equal(pq_arr, xyz_arr):
                            logger.error(
                                f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                                f"List/Array mismatch: Parquet={_format_val_summary(pq_val)} != XYZ={_format_val_summary(xyz_val)}.{restart_tag}"
                            )
                            mismatches += 1
                            mismatches_value_diff += 1
            else:
                if isinstance(pq_val, (bool, np.bool_)) or isinstance(xyz_val, (bool, np.bool_)):
                    if not (isinstance(pq_val, (bool, np.bool_)) and isinstance(xyz_val, (bool, np.bool_))) or bool(pq_val) != bool(xyz_val):
                        logger.error(
                            f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                            f"Boolean mismatch: Parquet={pq_val!r} != XYZ={xyz_val!r}.{restart_tag}"
                        )
                        mismatches += 1
                        mismatches_value_diff += 1
                elif isinstance(pq_val, (int, float, np.number)) and isinstance(xyz_val, (int, float, np.number)):
                    if isinstance(pq_val, (float, np.floating)) or isinstance(xyz_val, (float, np.floating)):
                        if not math.isclose(float(pq_val), float(xyz_val), rel_tol=1e-9, abs_tol=1e-12):
                            logger.error(
                                f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                                f"Numeric mismatch: Parquet={pq_val} != XYZ={xyz_val} (diff={abs(float(pq_val) - float(xyz_val))}).{restart_tag}"
                            )
                            mismatches += 1
                            mismatches_value_diff += 1
                    else:
                        if pq_val != xyz_val:
                            logger.error(
                                f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                                f"Integer mismatch: Parquet={pq_val!r} != XYZ={xyz_val!r}.{restart_tag}"
                            )
                            mismatches += 1
                            mismatches_value_diff += 1
                else:
                    if pq_val != xyz_val:
                        logger.error(
                            f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: "
                            f"Value mismatch: Parquet={pq_val!r} != XYZ={xyz_val!r}.{restart_tag}"
                        )
                        mismatches += 1
                        mismatches_value_diff += 1

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
        if mismatches_parquet_has_data_xyz_missing > 0:
            logger.error(
                f"   - {mismatches_parquet_has_data_xyz_missing} property instances: data present in Parquet but missing from ExtXYZ `atoms.info`."
            )
        if mismatches_xyz_has_data_parquet_missing > 0:
            logger.error(
                f"   - {mismatches_xyz_has_data_parquet_missing} property instances: data present in ExtXYZ `atoms.info` but missing from Parquet."
            )
        if mismatches_value_diff > 0:
            logger.error(
                f"   - {mismatches_value_diff} property instances: values differ between Parquet and ExtXYZ."
            )


if __name__ == "__main__":
    main()
