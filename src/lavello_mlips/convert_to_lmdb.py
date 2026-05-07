import argparse
import logging
from pathlib import Path
from typing import List

from ase.io import write
from tqdm import tqdm

from .aselmdb import LMDBDatabase
from .utils import setup_logging

logger = logging.getLogger(__name__)


def cv_xyz_to_lmdb(input_files: List[str], output_file: str):
    """Convert multiple Extended XYZ files to one LMDB database."""
    path = Path(output_file)
    if path.suffix != ".aselmdb":
        if path.suffix == ".lmdb":
            output_file = str(path.with_suffix(".aselmdb"))
        else:
            output_file = str(path.parent / (path.name + ".aselmdb"))
        logger.warning(
            f"Changing output extension to .aselmdb for Fairchem compatibility: {output_file}"
        )

    Path(output_file).unlink(missing_ok=True)

    with LMDBDatabase(output_file) as db:
        for input_file in input_files:
            logger.info(f"Reading {input_file}...")
            from ase.io import read

            atoms_list = read(input_file, format="extxyz", index=":")
            if not isinstance(atoms_list, list):
                atoms_list = [atoms_list]
            logger.info(f"Found {len(atoms_list)} structures.")
            for atoms in tqdm(
                atoms_list, desc=f"Writing {Path(input_file).name} to LMDB"
            ):
                db.write(atoms)

    logger.info(f"Successfully wrote {output_file}")


def cv_lmdb_to_xyz(input_file: str, output_file: str):
    """Convert an LMDB database back to an Extended XYZ file."""
    logger.info(f"Opening LMDB database {input_file}...")
    with LMDBDatabase(input_file, readonly=True) as db:
        ids = db.ids
        logger.info(f"Found {len(ids)} structures in database.")

        all_atoms = []
        for idx in tqdm(ids, desc="Reading from LMDB"):
            atoms = db.get_atoms(idx)
            all_atoms.append(atoms)

        logger.info(f"Writing {len(all_atoms)} structures to {output_file}...")
        write(output_file, all_atoms, format="extxyz")

    logger.info(f"Successfully wrote {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert between Extended XYZ and ASE-style LMDB database."
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # to-lmdb
    tolmdb = subparsers.add_parser("to-lmdb", help="Convert XYZ to LMDB")
    tolmdb.add_argument("input_files", nargs="+", help="Input Extended XYZ files")
    tolmdb.add_argument(
        "-o", "--output", required=True, help="Output LMDB file (.aselmdb)"
    )

    # from-lmdb
    fromlmdb = subparsers.add_parser("from-lmdb", help="Convert LMDB to XYZ")
    fromlmdb.add_argument("input_file", help="Input LMDB file (.aselmdb)")
    fromlmdb.add_argument(
        "-o", "--output", required=True, help="Output Extended XYZ file"
    )

    args = parser.parse_args()
    setup_logging()

    if args.command == "to-lmdb":
        cv_xyz_to_lmdb(args.input_files, args.output)
    elif args.command == "from-lmdb":
        cv_lmdb_to_xyz(args.input_file, args.output)
    else:
        # Fallback for old usage: if only input and output are provided WITHOUT subcommands
        # But since I changed the parser, I should probably keep it compatible if possible
        # or just stick to the new cleaner CLI.
        parser.print_help()


if __name__ == "__main__":
    main()
