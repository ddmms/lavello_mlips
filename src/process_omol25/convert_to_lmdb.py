import argparse
import logging
import os
import zlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import ase.db.core
import ase.db.row
from ase import Atoms
from ase.io import read
from ase.io.jsonio import decode, encode
from ase.calculators.singlepoint import SinglePointCalculator
import lmdb
import numpy as np
from tqdm import tqdm

# Special keys in aselmdb
RESERVED_KEYS = ["nextid", "deleted_ids", "metadata"]

class LMDBDatabase(ase.db.core.Database):
    """
    LMDB backend for ASE database, compatible with ase-db-backends aselmdb.
    Used by fairchem.core.datasets.AseDBDataset.
    """
    def __init__(self, filename: Union[str, Path], readonly: bool = False, **kwargs):
        super().__init__(filename)
        self.filename = Path(filename)
        self.readonly = readonly
        self._env = None
        self._open_lmdb_env()
        
        # Load all ids based on keys in the DB.
        self.ids = []
        self.deleted_ids = set()
        self._load_ids()

    def _open_lmdb_env(self):
        self._env = lmdb.open(
            str(self.filename),
            map_size=2**41,  # 2Tb max size
            subdir=False,
            meminit=False,
            map_async=True,
            readonly=self.readonly,
            lock=not self.readonly,
        )

    def __enter__(self):
        self.txn = self._env.begin(write=not self.readonly)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if not self.readonly:
            self.txn.commit()
        self._env.close()

    def close(self):
        if self._env:
            self._env.close()

    def _get_txn(self, write: bool):
        if hasattr(self, "txn"):
            return self.txn
        return self._env.begin(write=write)

    def _flush_txn(self, txn):
        if not hasattr(self, "txn"):
            txn.commit()

    @property
    def _nextid(self) -> int:
        txn = self._get_txn(write=False)
        try:
            nextid_data = txn.get("nextid".encode("ascii"))
            return decode_bytestream(nextid_data, json_decode=True) if nextid_data else 1
        finally:
            self._flush_txn(txn)

    def _load_ids(self):
        txn = self._get_txn(write=False)
        try:
            deleted_ids_data = txn.get("deleted_ids".encode("ascii"))
            if deleted_ids_data:
                self.deleted_ids = set(decode_bytestream(deleted_ids_data, json_decode=True))
        finally:
            self._flush_txn(txn)
        
        self.ids = [i for i in range(1, self._nextid) if i not in self.deleted_ids]

    def _write(self, atoms: Union[Atoms, ase.db.row.AtomsRow], key_value_pairs=None, data=None, idx=None):
        if isinstance(atoms, ase.db.row.AtomsRow):
            row = atoms
        else:
            row = ase.db.row.AtomsRow(atoms)
            row.ctime = ase.db.core.now()
            row.user = os.getenv("USER")

        dct = {}
        for key in row.__dict__:
            if key[0] == "_" or key in row._keys or key == "id":
                continue
            dct[key] = row[key]
        
        if key_value_pairs:
            dct["key_value_pairs"] = key_value_pairs
        if data:
            dct["data"] = data
        else:
            dct["data"] = {}
            
        # Cell conversion for JSON
        dct["cell"] = np.asarray(dct["cell"])
        
        txn = self._get_txn(write=True)
        try:
            if idx is None:
                idx = self._nextid
                nextid = idx + 1
            else:
                nextid = max(idx + 1, self._nextid)

            txn.put(
                str(idx).encode("ascii"),
                encode_object(dct, json_encode=True)
            )
            txn.put(
                "nextid".encode("ascii"),
                encode_object(nextid, json_encode=True)
            )
            if idx not in self.ids:
                self.ids.append(idx)
            return idx
        finally:
            self._flush_txn(txn)

    def _get_row(self, idx: int):
        txn = self._get_txn(write=False)
        try:
            row_data = txn.get(str(idx).encode("ascii"))
        finally:
            self._flush_txn(txn)
            
        if row_data is None:
            raise KeyError(f"ID {idx} not found")
            
        dct = decode_bytestream(row_data, json_decode=True)
        
        # Ensure calculator properties are numpy arrays
        from ase.calculators.calculator import all_properties
        for key in all_properties:
            if key in dct and isinstance(dct[key], list):
                dct[key] = np.array(dct[key])
        
        dct["id"] = idx
        return ase.db.row.AtomsRow(dct)

    def get_atoms(self, idx: int) -> Atoms:
        row = self._get_row(idx)
        atoms = row.toatoms()
        
        # Restore metadata to atoms.info
        if isinstance(row.data, dict):
            atoms.info.update(row.data)
            
        # Restore calculator properties
        results = {}
        for prop in ['energy', 'forces', 'stress']:
            val = getattr(row, prop, None)
            if val is not None:
                results[prop] = val
        
        if results:
            atoms.calc = SinglePointCalculator(atoms, **results)
            
        return atoms

def encode_object(obj: Any, compress=True, json_encode=True) -> bytes:
    """Encode object to compressed JSON."""
    if json_encode:
        obj = encode(obj)
    if compress:
        return zlib.compress(obj.encode("utf-8"))
    return obj.encode("utf-8")

def decode_bytestream(bytestream: bytes, decompress=True, json_decode=True) -> Any:
    """Decode compressed JSON bytestream."""
    if decompress:
        bytestream = zlib.decompress(bytestream).decode("utf-8")
    else:
        bytestream = bytestream.decode("utf-8")
    if json_decode:
        return decode(bytestream)
    return bytestream

def cv_xyz_to_lmdb(input_files: List[str], output_file: str):
    """Convert multiple XYZ files to one LMDB database."""
    # Ensure output has .aselmdb extension for fairchem compatibility.
    # .lmdb is not recognized by ase.db.connect, so we force .aselmdb.
    path = Path(output_file)
    if path.suffix != ".aselmdb":
        if path.suffix == ".lmdb":
            output_file = str(path.with_suffix(".aselmdb"))
        else:
            output_file = str(path.parent / (path.name + ".aselmdb"))
        print(f"Warning: Changing output extension to .aselmdb for Fairchem compatibility: {output_file}")
        
    # Remove existing file to start fresh
    Path(output_file).unlink(missing_ok=True)
    
    with LMDBDatabase(output_file) as db:
        for input_file in input_files:
            print(f"Reading {input_file}...")
            atoms_list = read(input_file, index=":")
            print(f"Found {len(atoms_list)} structures.")
            for atoms in tqdm(atoms_list, desc=f"Writing {Path(input_file).name} to LMDB"):
                db.write(atoms)
                
    print(f"Successfully wrote {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Convert Extended XYZ files to ASE-style LMDB database (Fairchem compatible).")
    parser.add_argument("input_files", nargs="+", help="Input Extended XYZ files")
    parser.add_argument("-o", "--output", required=True, help="Output LMDB file (suggested extension: .aselmdb)")
    args = parser.parse_args()
    
    cv_xyz_to_lmdb(args.input_files, args.output)

if __name__ == "__main__":
    main()
