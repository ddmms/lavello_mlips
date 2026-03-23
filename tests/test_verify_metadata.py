import pytest
from pathlib import Path
from ase import Atoms
from ase.io import write
from lavello_mlips.convert_to_lmdb import cv_xyz_to_lmdb

@pytest.fixture
def sample_aselmdb(tmp_path):
    """Fixture to create a sample ASLMDB with metadata."""
    xyz_path = tmp_path / "sample.xyz"
    lmdb_path = tmp_path / "sample.aselmdb"
    
    atoms = Atoms('H2', positions=[[0, 0, 0], [0, 0, 0.74]])
    atoms.info['energy'] = -1.0
    atoms.info['string_val'] = "hello"
    atoms.info['list_val'] = [1.0, 2.0, 3.0]
    atoms.info['dict_val'] = {"k1": "v1", "k2": 2}
    
    write(str(xyz_path), atoms, format="extxyz")
    cv_xyz_to_lmdb([str(xyz_path)], str(lmdb_path))
    
    return lmdb_path

def test_verify_fairchem(sample_aselmdb):
    """Verify metadata retrieval using Fairchem's AseDBDataset if available."""
    try:
        from fairchem.core.datasets import AseDBDataset
    except (ImportError, ModuleNotFoundError):
        pytest.skip("Fairchem not found in this environment.")

    dataset = AseDBDataset({"src": str(sample_aselmdb)})
    atoms = dataset.get_atoms(0)
    
    assert atoms.get_chemical_formula() == "H2"
    assert atoms.info["energy"] == -1.0
    assert atoms.info["string_val"] == "hello"
    assert list(atoms.info["list_val"]) == [1.0, 2.0, 3.0]
    # Check dict structure
    assert atoms.info["dict_val"]["k1"] == "v1"
    assert atoms.info["dict_val"]["k2"] == 2

def test_verify_mace(sample_aselmdb):
    """Verify metadata retrieval using MACE's AseDBDataset if available."""
    try:
        from mace.tools.fairchem_dataset import AseDBDataset as MaceDataset
    except (ImportError, ModuleNotFoundError):
        pytest.skip("MACE not found in this environment.")

    dataset = MaceDataset({"src": str(sample_aselmdb)})
    atoms = dataset.get_atoms(0)
    
    assert atoms.get_chemical_formula() == "H2"
    assert atoms.info["energy"] == -1.0
    assert atoms.info["string_val"] == "hello"
    assert list(atoms.info["list_val"]) == [1.0, 2.0, 3.0]
    assert atoms.info["dict_val"]["k1"] == "v1"
    assert atoms.info["dict_val"]["k2"] == 2
