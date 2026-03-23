import os
from pathlib import Path
import numpy as np
import pytest
from ase import Atoms
from ase.io import read
from lavello_mlips.aselmdb import LMDBDatabase
from lavello_mlips.convert_to_lmdb import cv_xyz_to_lmdb

def test_noble_gas_realistic_conversion(tmp_path):
    """
    Perform a realistic conversion test using structs_noble_gas_compounds.xyz
    and verify high-fidelity preservation of properties and metadata.
    """
    project_root = Path(__file__).parent.parent
    data_xyz = project_root / "data" / "structs_noble_gas_compounds.xyz"
    output_lmdb = tmp_path / "noble_gas_realistic.aselmdb"
    
    if not data_xyz.exists():
        pytest.skip(f"Data file {data_xyz} not found")
        
    # 1. Read original data
    original_atoms = read(str(data_xyz), index=":")
    num_configs = len(original_atoms)
    
    # 2. Convert to LMDB
    cv_xyz_to_lmdb([str(data_xyz)], str(output_lmdb))
    
    # 3. Verify integrity using LMDBDatabase
    with LMDBDatabase(output_lmdb, readonly=True) as db:
        assert len(db.ids) == num_configs
        assert db._nextid == num_configs + 1
        
        # Verify first atom in detail (HCNKrF+)
        a1 = db.get_atoms(1)
        orig1 = original_atoms[0]
        
        assert a1.get_chemical_formula() == orig1.get_chemical_formula()
        assert np.allclose(a1.positions, orig1.positions)
        
        # Verify Energy (Standard ASE property)
        # In XYZ it is 'energy=-80175.9532076234'
        assert np.isclose(a1.get_potential_energy(), orig1.get_potential_energy())
        
        # Verify Forces
        assert np.allclose(a1.get_forces(), orig1.get_forces())
        assert "forces" in a1.calc.results
        
        # Verify specialized metadata (scalars)
        expected_scalars = {
            "homo_eV": -19.1842,
            "lumo_eV": -7.6401,
            "gap_eV": 11.5441,
            "total_charge_e": 1,
            "multiplicity": 1,
            "argonne_rel": "noble_gas_compounds/HCNKrF+_step25_1_1/"
        }
        for k, v in expected_scalars.items():
            assert k in a1.info
            if isinstance(v, float):
                assert np.isclose(a1.info[k], v)
            else:
                assert a1.info[k] == v
                
        # Verify combined dipole (parsed from string in XYZ to info in LMDB)
        # dipole="-0.2545022579856644 1.3906404658696951 -0.05132152150198356"
        assert "dipole" in a1.info or "dipole" in a1.calc.results
        # Note: if it's a string in XYZ, it might be a string or list/array in ASE depending on how it was read.
        # read(format='extxyz') usually handles pbc/cell as special, but 'dipole' as info.
        
        # Check a random atom middle of the set
        mid_idx = num_configs // 2
        a_mid = db.get_atoms(mid_idx + 1)
        orig_mid = original_atoms[mid_idx]
        assert a_mid.get_chemical_formula() == orig_mid.get_chemical_formula()
        assert np.isclose(a_mid.get_potential_energy(), orig_mid.get_potential_energy())

def test_aselmdb_metadata_reserved_keys(tmp_path):
    """Verify that internal metadata like 'nextid' is correctly managed."""
    lmdb_path = tmp_path / "metadata_test.aselmdb"
    
    with LMDBDatabase(lmdb_path, readonly=False) as db:
        # DB should start with nextid=1
        assert db._nextid == 1
        
        # Add one atom
        a = Atoms('H', positions=[[0,0,0]])
        db.write(a)
        assert db._nextid == 2
        
        # Add another
        db.write(a)
        assert db._nextid == 3
        
    # Reopen and check
    with LMDBDatabase(lmdb_path, readonly=True) as db:
        assert len(db.ids) == 2
        assert db._nextid == 3
        # Check keys in txn explicitly
        txn = db._env.begin()
        keys = [k.decode('ascii') for k, _ in txn.cursor()]
        assert "nextid" in keys
        assert "1" in keys
        assert "2" in keys
