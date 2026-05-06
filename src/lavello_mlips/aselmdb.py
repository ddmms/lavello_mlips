import logging
import os
import zlib
from pathlib import Path
from typing import Any, Union

import ase.db.core
import ase.db.row
from ase import Atoms
from ase.io.jsonio import decode, encode
from ase.calculators.singlepoint import SinglePointCalculator
import lmdb
import numpy as np
import orjson

logger = logging.getLogger(__name__)

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
            return (
                decode_bytestream(nextid_data, json_decode=True) if nextid_data else 1
            )
        finally:
            self._flush_txn(txn)

    def _load_ids(self):
        txn = self._get_txn(write=False)
        try:
            deleted_ids_data = txn.get("deleted_ids".encode("ascii"))
            if deleted_ids_data:
                self.deleted_ids = set(
                    decode_bytestream(deleted_ids_data, json_decode=True)
                )
        finally:
            self._flush_txn(txn)

        self.ids = [i for i in range(1, self._nextid) if i not in self.deleted_ids]

    def _write(
        self,
        atoms: Union[Atoms, ase.db.row.AtomsRow],
        key_value_pairs=None,
        data=None,
        idx=None,
    ):
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

        # Capture atoms.info into key_value_pairs (scalars) and data (non-scalars)
        import numbers

        scalar_types = (numbers.Real, str, bool, np.bool_)
        for k, v in atoms_obj.info.items():
            if isinstance(v, scalar_types):
                key_value_pairs[k] = v
            else:
                data.setdefault("__info__", {})[k] = v

        # Capture custom arrays into data
        standard_arrays = {
            "numbers",
            "positions",
            "tags",
            "momenta",
            "masses",
            "charges",
            "magmoms",
            "velocities",
        }
        arrays_to_dump = {
            k: v for k, v in atoms_obj.arrays.items() if k not in standard_arrays
        }
        if arrays_to_dump:
            data.setdefault("__arrays__", {}).update(arrays_to_dump)

        # Standard ASE db fields
        dct = {
            "numbers": atoms_obj.get_atomic_numbers(),
            "positions": atoms_obj.get_positions(),
            "cell": np.asarray(atoms_obj.get_cell()),
            "pbc": atoms_obj.get_pbc(),
            "mtime": ase.db.core.now(),
            "ctime": getattr(row, "ctime", ase.db.core.now()),
            "user": getattr(row, "user", os.getenv("USER")),
            "key_value_pairs": key_value_pairs,
            "data": data,
        }

        # Capture calculator properties
        if atoms_obj.calc is not None:
            for prop, val in atoms_obj.calc.results.items():
                if val is not None:
                    dct[prop] = val

        if atoms_obj.get_tags().any():
            dct["tags"] = atoms_obj.get_tags()

        # Cell conversion for JSON
        dct["cell"] = np.asarray(dct["cell"])

        txn = self._get_txn(write=True)
        try:
            if idx is None:
                idx = self._nextid
                nextid = idx + 1
            else:
                nextid = max(idx + 1, self._nextid)

            txn.put(str(idx).encode("ascii"), encode_object(dct, json_encode=True))
            txn.put("nextid".encode("ascii"), encode_object(nextid, json_encode=True))
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

        # Restore scalars from key_value_pairs (ase-db doesn't always put them back in info)
        if hasattr(row, "key_value_pairs") and isinstance(row.key_value_pairs, dict):
            atoms.info.update(row.key_value_pairs)

        # Restore non-scalars from data['__info__'] and custom arrays from data['__arrays__']
        if isinstance(row.data, dict):
            if "__info__" in row.data:
                atoms.info.update(row.data["__info__"])
            if "__arrays__" in row.data:
                for k, v in row.data["__arrays__"].items():
                    if k not in atoms.arrays:
                        atoms.new_array(k, np.array(v))

            # Also restore any other keys in data that are not the special ones
            for k, v in row.data.items():
                if k not in ["__info__", "__arrays__"]:
                    atoms.info[k] = v

        # Restore calculator properties
        results = {}
        # standard properties and any others stored in dct
        from ase.calculators.calculator import all_properties

        for prop in list(all_properties) + ["free_energy"]:
            val = getattr(row, prop, None)
            if val is not None:
                results[prop] = val

        # If still no energy, check info (historical/historical compatibility)
        if "energy" not in results and "energy" in atoms.info:
            results["energy"] = atoms.info["energy"]

        if results:
            atoms.calc = SinglePointCalculator(atoms, **results)

        return atoms


def encode_object(obj: Any, compress=True, json_encode=True) -> bytes:
    """Encode object to compressed JSON."""
    if json_encode:
        try:
            # OPT_SERIALIZE_NUMPY handles numpy arrays directly
            obj_bytes = orjson.dumps(obj, option=orjson.OPT_SERIALIZE_NUMPY)
        except orjson.JSONEncodeError:
            # Fallback to standard ASE jsonio if orjson fails (e.g. for unsupported complex objects)
            obj_bytes = encode(obj).encode("utf-8")
    else:
        obj_bytes = obj.encode("utf-8") if isinstance(obj, str) else bytes(obj)

    if compress:
        return zlib.compress(obj_bytes)
    return obj_bytes


def decode_bytestream(bytestream: bytes, decompress=True, json_decode=True) -> Any:
    """Decode compressed JSON bytestream."""
    if decompress:
        bytestream = zlib.decompress(bytestream)

    if json_decode:
        # ASE's custom JSON encoder uses special keys like __ndarray__, __complex__, etc.
        # If the payload contains these, we must use ASE's decoder to reconstruct the objects.
        if b"__ndarray__" in bytestream or b"__complex__" in bytestream:
            return decode(bytestream.decode("utf-8"))

        try:
            return orjson.loads(bytestream)
        except orjson.JSONDecodeError:
            # Fallback to standard ASE jsonio if orjson fails
            return decode(bytestream.decode("utf-8"))

    return bytestream.decode("utf-8")
