import argparse
import logging
import os
import zlib
from pathlib import Path
import numbers
from typing import Any, Dict, List, Optional, Union

import ase.db.core
import ase.db.row
from ase.io import read, write
import lmdb
import numpy as np
import orjson
from tqdm import tqdm

class LMDBDatabase(ase.db.core.Database):
    """
    LMDB backend for ASE database, adapted from MACE/fairchem.
    This allows storing ASE Atoms objects in an LMDB file with near-instant access.
    """
    def __init__(
        self,
        filename: Union[str, Path],
        readonly: bool = False,
        **kwargs
    ) -> None:
        super().__init__(Path(filename), **kwargs)
        self.readonly = readonly

        if self.readonly:
            self.env = lmdb.open(
                str(self.filename),
                subdir=False,
                readonly=True,
                lock=False,
                meminit=False,
                map_async=True,
            )
            self.txn = self.env.begin(write=False)
        else:
            self.env = lmdb.open(
                str(self.filename),
                map_size=1099511627776, # 1TB
                subdir=False,
                meminit=False,
                map_async=True,
            )
            self.txn = self.env.begin(write=True)

        self.ids = []
        self.deleted_ids = []
        self._load_ids()

    def _load_ids(self) -> None:
        deleted_ids_data = self.txn.get("deleted_ids".encode("ascii"))
        if deleted_ids_data is not None:
            self.deleted_ids = orjson.loads(zlib.decompress(deleted_ids_data))
        
        nextid_data = self.txn.get("nextid".encode("ascii"))
        nextid = orjson.loads(zlib.decompress(nextid_data)) if nextid_data else 1
        self.ids = [i for i in range(1, nextid) if i not in set(self.deleted_ids)]

    def _get_nextid(self) -> int:
        nextid_data = self.txn.get("nextid".encode("ascii"))
        if nextid_data:
            return orjson.loads(zlib.decompress(nextid_data))
        return 1

    def _write(
        self,
        atoms: Union[ase.Atoms, ase.db.row.AtomsRow],
        key_value_pairs: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        id: Optional[int] = None,
    ) -> int:
        if isinstance(atoms, ase.db.row.AtomsRow):
            row = atoms
            atoms_obj = row.toatoms()
        else:
            atoms_obj = atoms
            row = ase.db.row.AtomsRow(atoms_obj)
            row.ctime = ase.db.core.now()
            row.user = os.getenv("USER")

        if key_value_pairs is None:
            key_value_pairs = {}
        if data is None:
            data = {}

        # MACE logic: atoms.info -> key_value_pairs (scalars) or data (non-scalars)
        scalar_types = (numbers.Real, str, bool, np.bool_)
        for k, v in atoms_obj.info.items():
            if isinstance(v, scalar_types):
                key_value_pairs[k] = v
            else:
                data.setdefault("__info__", {})[k] = v

        # MACE logic: atoms.arrays -> data (except standard ones)
        standard_arrays = {
            "numbers", "positions", "tags", "momenta", "masses",
            "charges", "magmoms", "velocities"
        }
        arrays_to_dump = {
            k: v for k, v in atoms_obj.arrays.items() if k not in standard_arrays
        }
        if arrays_to_dump:
            data.setdefault("__arrays__", {}).update(arrays_to_dump)

        dct = {
            "numbers": atoms_obj.get_atomic_numbers(),
            "positions": atoms_obj.get_positions(),
            "cell": np.asarray(atoms_obj.get_cell()),
            "pbc": atoms_obj.get_pbc(),
            "key_value_pairs": key_value_pairs,
            "data": data,
            "mtime": ase.db.core.now(),
            "ctime": getattr(row, 'ctime', ase.db.core.now()),
            "user": getattr(row, 'user', os.getenv("USER")),
        }

        # Capture standard properties if they exist
        try:
            dct["energy"] = atoms_obj.get_potential_energy()
        except (RuntimeError, AttributeError):
            pass

        try:
            dct["forces"] = atoms_obj.get_forces()
        except (RuntimeError, AttributeError):
            pass
        
        try:
            dct["stress"] = atoms_obj.get_stress()
        except (RuntimeError, AttributeError):
            pass

        if atoms_obj.get_tags().any():
            dct["tags"] = atoms_obj.get_tags()

        if id is None:
            id = self._get_nextid()
            nextid = id + 1
        else:
            nextid = max(id + 1, self._get_nextid())

        # Serialize and write
        serialized = orjson.dumps(dct, option=orjson.OPT_SERIALIZE_NUMPY)
        compressed = zlib.compress(serialized)
        self.txn.put(f"{id}".encode("ascii"), compressed)

        if id not in self.ids:
            self.ids.append(id)
            self.txn.put(
                "nextid".encode("ascii"),
                zlib.compress(orjson.dumps(nextid, option=orjson.OPT_SERIALIZE_NUMPY))
            )
        
        return id

    def _get_dct(self, id: int) -> Dict[str, Any]:
        data = self.txn.get(f"{id}".encode("ascii"))
        if data is None:
            raise KeyError(f"ID {id} not found in database")
        return orjson.loads(zlib.decompress(data))

    def get_atoms(self, id: int) -> ase.Atoms:
        dct = self._get_dct(id)
        
        atoms = ase.Atoms(
            numbers=np.asarray(dct['numbers']),
            positions=np.asarray(dct['positions']),
            cell=np.asarray(dct['cell']),
            pbc=dct['pbc']
        )
        if 'tags' in dct:
            atoms.set_tags(np.asarray(dct['tags']))

        # Restore standard properties via calculator
        calc_results = {}
        for prop in ['energy', 'forces', 'stress']:
            if prop in dct:
                calc_results[prop] = np.asarray(dct[prop])
        
        if calc_results:
            from ase.calculators.singlepoint import SinglePointCalculator
            atoms.calc = SinglePointCalculator(atoms, **calc_results)
            # Also put in info for convenience (and to match the test's expectations)
            for k, v in calc_results.items():
                atoms.info[k] = v

        # Restore info
        if 'key_value_pairs' in dct:
            atoms.info.update(dct['key_value_pairs'])
        
        # Restore data contents to info and arrays
        if 'data' in dct:
            data_dict = dct['data']
            if '__info__' in data_dict:
                # Restore non-scalar info, ensuring nested dictionaries are handled
                atoms.info.update(data_dict['__info__'])
            if '__arrays__' in data_dict:
                for k, v in data_dict['__arrays__'].items():
                    atoms.new_array(k, np.asarray(v))
            
            # Rest of data goes to info
            for k, v in data_dict.items():
                if k not in ['__info__', '__arrays__']:
                    atoms.info[k] = v
        
        return atoms

    def close(self) -> None:
        if not self.readonly:
            self.txn.commit()
        self.env.close()

    def __enter__(self) -> "LMDBDatabase":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

def _decode_ndarrays(obj):
    """Recursively turn {"__ndarray__": [...] } blobs back into NumPy arrays."""
    if isinstance(obj, dict):
        if "__ndarray__" in obj:
            shape, dtype, flat = obj["__ndarray__"]
            return np.asarray(flat, dtype=dtype).reshape(shape)
        return {k: _decode_ndarrays(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decode_ndarrays(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_decode_ndarrays(v) for v in obj)
    return obj

def cv_xyz_to_lmdb(input_files: Union[str, List[str]], output_file: str):
    """Converts one or more extxyz files to a single LMDB."""
    if isinstance(input_files, str):
        input_files = [input_files]

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
    parser = argparse.ArgumentParser(description="Convert extxyz to LMDB")
    parser.add_argument("inputs", nargs="+", help="Input extxyz file(s)")
    parser.add_argument("output", help="Output LMDB file")
    args = parser.parse_args()

    cv_xyz_to_lmdb(args.inputs, args.output)

if __name__ == "__main__":
    main()
