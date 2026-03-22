import os
from pathlib import Path
import numpy as np
from process_omol25.convert_to_lmdb import LMDBDatabase, cv_xyz_to_lmdb
from ase import Atoms
from ase.io import write

def test_xyz2lmdb_conversion():
    input_xyz1 = "tests/sample1.xyz"
    input_xyz2 = "tests/sample2.xyz"
    output_lmdb = "tests/sample.lmdb"
    

    
    # Create sample 1
    atoms1 = Atoms('H2', positions=[[0, 0, 0], [0, 0, 0.74]])
    atoms1.info['energy'] = -1.0
    write(input_xyz1, [atoms1], format="extxyz")
    
    # Create sample 2
    atoms2 = Atoms('O2', positions=[[0, 0, 0], [0, 0, 1.21]])
    atoms2.info['energy'] = -2.0
    write(input_xyz2, [atoms2], format="extxyz")

    # Run conversion with multiple inputs
    cv_xyz_to_lmdb([input_xyz1, input_xyz2], output_lmdb)
    
    assert Path(output_lmdb).exists()
    
    # Verify LMDB
    with LMDBDatabase(output_lmdb, readonly=True) as db:
        assert len(db.ids) == 2
        
        read_atoms1 = db.get_atoms(1)
        print(f"Read atoms 1 info: {read_atoms1.info}")
        assert read_atoms1.get_chemical_formula() == 'H2'
        assert read_atoms1.info['energy'] == -1.0
        
        read_atoms2 = db.get_atoms(2)
        assert read_atoms2.get_chemical_formula() == 'O2'
        assert read_atoms2.info['energy'] == -2.0

    # Cleanup
    for f in [input_xyz1, input_xyz2, output_lmdb]:
        Path(f).unlink(missing_ok=True)

if __name__ == "__main__":
    test_xyz2lmdb_conversion()
    print("Test passed!")
