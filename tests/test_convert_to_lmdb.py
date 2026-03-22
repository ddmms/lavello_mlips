import os
from pathlib import Path
import numpy as np
from process_omol25.convert_to_lmdb import LMDBDatabase, cv_xyz_to_lmdb
from ase import Atoms
from ase.io import read, write

def test_convert_to_lmdb_aselmdb_format():
    input_xyz1 = "tests/sample1.xyz"
    input_xyz2 = "tests/sample2.xyz"
    output_lmdb = "tests/sample.aselmdb"
    
    # Clean up
    Path(input_xyz1).unlink(missing_ok=True)
    Path(input_xyz2).unlink(missing_ok=True)
    Path(output_lmdb).unlink(missing_ok=True)
    
    # Create sample 1
    atoms1 = Atoms('H2', positions=[[0, 0, 0], [0, 0, 0.74]])
    atoms1.info['energy'] = -1.0
    atoms1.info['string_val'] = "hello"
    atoms1.info['list_val'] = [1.0, 2.0, 3.0]
    atoms1.info['dict_val'] = {"a": 1, "b": {"c": 2}}
    write(input_xyz1, atoms1,format="extxyz")
    
    # Create sample 2
    atoms2 = Atoms('O2', positions=[[0, 0, 0], [0, 0, 1.2]])
    atoms2.info['energy'] = -2.0
    write(input_xyz2, atoms2,format="extxyz")
    
    # Convert
    cv_xyz_to_lmdb([input_xyz1, input_xyz2], output_lmdb)
    
    # Verify existence
    assert Path(output_lmdb).exists()
    
    # Verify content using our LMDBDatabase
    with LMDBDatabase(output_lmdb, readonly=True) as db:
        assert len(db.ids) == 2
        # aselmdb IDs are 1-based
        assert 1 in db.ids
        assert 2 in db.ids
        
        a1 = db.get_atoms(1)
        a2 = db.get_atoms(2)
        
        assert a1.get_chemical_formula() == 'H2'
        assert a1.calc.results['energy'] == -1.0
        
        assert a2.get_chemical_formula() == 'O2'
        assert a2.calc.results['energy'] == -2.0
        
    print("Basic aselmdb test passed!")

if __name__ == "__main__":
    test_convert_to_lmdb_aselmdb_format()
