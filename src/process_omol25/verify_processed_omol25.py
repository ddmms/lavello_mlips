"""
Standalone utility for cross-referencing Parquet files with their corresponding ExtXYZ files.
Verifies that all properties stored in the Parquet dataset perfectly match the data embedded
in `atoms.info`.
"""
import argparse
import math
from pathlib import Path

import pandas as pd
import numpy as np
from ase.io import read

def parse_args():
    parser = argparse.ArgumentParser(
        description="Verify consistency between a Parquet dataset and its corresponding ExtXYZ file."
    )
    parser.add_argument(
        "--parquet",
        type=Path,
        required=True,
        help="Path to the .parquet file to check."
    )
    parser.add_argument(
        "--extxyz",
        type=Path,
        required=True,
        help="Path to the corresponding .xyz file to cross-reference."
    )
    parser.add_argument(
        "--skip-keys",
        nargs="*",
        default=["process_time_s", "argonne_rel", "data_id"],
        help="Properties to skip during exact matching."
    )
    return parser.parse_args()

def main():
    args = parse_args()

    if not args.parquet.exists():
        print(f"Error: Parquet file '{args.parquet}' does not exist.")
        sys.exit(1)
    if not args.extxyz.exists():
        print(f"Error: ExtXYZ file '{args.extxyz}' does not exist.")
        sys.exit(1)

    print(f"Loading Parquet file from {args.parquet}...")
    df = pd.read_parquet(args.parquet)
    if "geom_sha1" not in df.columns:
        print("Error: 'geom_sha1' key is missing from Parquet file. Cannot perform alignment.")
        sys.exit(1)

    print(f"Loaded {len(df)} records from Parquet.")
    parquet_by_sha = {row["geom_sha1"]: row for _, row in df.iterrows()}
    parquet_by_argone_rel = {row["argonne_rel"]: row for _, row in df.iterrows()}
    print(f"Loading ExtXYZ file from {args.extxyz} (this may take a moment)...")
    all_atoms = read(str(args.extxyz), index=":")
    if not isinstance(all_atoms, list):
        all_atoms = [all_atoms] if all_atoms else []
    print(f"Loaded {len(all_atoms)} structures from ExtXYZ.")

    xyz_by_sha = {}
    xyz_by_argone_rel = {}
    for i, at in enumerate(all_atoms):
        sha = at.info.get("geom_sha1")
        if not sha:
            print(f"Warning: ExtXYZ frame {i} is missing 'geom_sha1' in atoms.info.")
            continue
        xyz_by_sha[sha] = at
        xyz_by_argone_rel[at.info.get("argonne_rel")] = at

    print("\n--- Structural Alignment by sha---")
    pq_keys = set(parquet_by_sha.keys())
    xyz_keys = set(xyz_by_sha.keys())
    pq_argonne_rels = set(parquet_by_argone_rel.keys())
    xyz_argonne_rels = set(xyz_by_argone_rel.keys())

    missing_in_xyz = pq_keys - xyz_keys
    missing_in_pq = xyz_keys - pq_keys
    missing_in_xyz_argonne_rel = pq_argonne_rels - xyz_argonne_rels
    missing_in_pq_argonne_rel = xyz_argonne_rels - pq_argonne_rels

    if missing_in_xyz:
        print(f"❌ {len(missing_in_xyz)} structures found in Parquet but missing from ExtXYZ.")
    if missing_in_pq:
        print(f"❌ {len(missing_in_pq)} structures found in ExtXYZ but missing from Parquet.")
    
    if not missing_in_xyz and not missing_in_pq:
        print(f"✅ Structural alignment perfect: both files contain exactly {len(pq_keys)} uniquely matched molecules.")

    print("\n--- Structural Alignment by argonne_rel ---")
    if missing_in_xyz_argonne_rel:
        print(f"❌ {len(missing_in_xyz_argonne_rel)} structures found in Parquet but missing from ExtXYZ.")
    if missing_in_pq_argonne_rel:
        print(f"❌ {len(missing_in_pq_argonne_rel)} structures found in ExtXYZ but missing from Parquet.")
    
    if not missing_in_xyz_argonne_rel and not missing_in_pq_argonne_rel:
        print(f"✅ Structural alignment perfect: both files contain exactly {len(pq_argonne_rels)} uniquely matched molecules.")
    
    print("\n--- Property Validation by argonne_rel ---")
    common_argonne_rels = pq_argonne_rels.intersection(xyz_argonne_rels)
    skip_keys = set(args.skip_keys)
    mismatches = 0
    checked_props = {key: 0 for key in df.columns if key not in skip_keys and key not in ("argonne_rel", "process_time_s") and "time" not in key.lower()}

    for argonne_rel in common_argonne_rels:
        row = parquet_by_argone_rel[argonne_rel]
        info = xyz_by_argone_rel[argonne_rel].info
        
        for key, pq_val in row.items():
            if key in skip_keys or key in ("argonne_rel", "process_time_s") or "time" in key.lower():
                continue
            
            if key not in info:
                print(f"  [Mismatch] argonne_rel={argonne_rel}: Key '{key}' missing from XYZ `atoms.info`.")
                mismatches += 1
                continue
            
            xyz_val = info[key]
            
            pq_is_nan = pq_val is None or (isinstance(pq_val, float) and math.isnan(pq_val))
            xyz_is_nan = xyz_val is None or (isinstance(xyz_val, float) and math.isnan(xyz_val))
            
            if pq_is_nan:
                if not xyz_is_nan:
                    print(f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: Parquet is NaN/None but XYZ has {xyz_val!r}")
                    mismatches += 1
            else:
                if isinstance(pq_val, float) and isinstance(xyz_val, float):
                    if not math.isclose(pq_val, xyz_val, rel_tol=1e-9, abs_tol=1e-12):
                        print(f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: Parquet={pq_val} != XYZ={xyz_val} (diff={abs(pq_val-xyz_val)})")
                        mismatches += 1
                elif pq_val != xyz_val:
                    try:
                        if isinstance(pq_val, np.ndarray) or isinstance(xyz_val, np.ndarray):
                            if not np.allclose(pq_val, xyz_val, equal_nan=True):
                                print(f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: NDArray mismatch")
                                mismatches += 1
                            continue
                        elif isinstance(pq_val, list) or isinstance(xyz_val, list):
                            if not np.allclose(np.array(pq_val), np.array(xyz_val), equal_nan=True):
                                print(f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: List/Array mismatch")
                                mismatches += 1
                            continue
                    except ImportError:
                        pass

                    print(f"  [Mismatch] argonne_rel={argonne_rel}, key={key}: Parquet={pq_val!r} != XYZ={xyz_val!r}")
                    mismatches += 1

            if key in checked_props:
                checked_props[key] += 1

    if mismatches == 0:
        print(f"\n✅ Data Consistency Verified! 0 mismatches found across {len(common_argonne_rels)} shared structures.")
    else:
        print(f"\n❌ Verification Failed. Encountered {mismatches} property inconsistencies.")
    
if __name__ == "__main__":
    main()
