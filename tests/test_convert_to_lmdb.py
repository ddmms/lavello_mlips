from pathlib import Path
import numpy as np
from lavello_mlips.aselmdb import LMDBDatabase
from lavello_mlips.convert_to_lmdb import cv_xyz_to_lmdb, cv_lmdb_to_xyz
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
    atoms1 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    atoms1.info["energy"] = -1.0
    atoms1.info["string_val"] = "hello"
    atoms1.info["list_val"] = [1.0, 2.0, 3.0]
    atoms1.info["dict_val"] = {"a": 1, "b": {"c": 2}}
    write(input_xyz1, atoms1, format="extxyz")

    # Create sample 2
    atoms2 = Atoms("O2", positions=[[0, 0, 0], [0, 0, 1.2]])
    atoms2.info["energy"] = -2.0
    write(input_xyz2, atoms2, format="extxyz")

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

        assert a1.get_chemical_formula() == "H2"
        assert a1.calc.results["energy"] == -1.0
        assert a1.info["string_val"] == "hello"

        assert a2.get_chemical_formula() == "O2"
        assert a2.calc.results["energy"] == -2.0

    # Clean up
    Path(input_xyz1).unlink(missing_ok=True)
    Path(input_xyz2).unlink(missing_ok=True)
    Path(output_lmdb).unlink(missing_ok=True)


def test_convert_lmdb_to_xyz():
    input_xyz = "tests/sample_for_reverse.xyz"
    temp_lmdb = "tests/temp_db.aselmdb"
    reverse_xyz = "tests/reversed.xyz"

    # Clean up
    for f in [input_xyz, temp_lmdb, reverse_xyz]:
        Path(f).unlink(missing_ok=True)

    # 1. Create source XYZ
    atoms1 = Atoms("H2", positions=[[0, 0, 0], [0, 0, 0.74]])
    atoms1.info["energy"] = -1.5
    atoms1.info["label"] = "test1"

    atoms2 = Atoms("O2", positions=[[0, 0, 0], [0, 0, 1.2]])
    atoms2.info["energy"] = -2.5
    atoms2.info["label"] = "test2"

    write(input_xyz, [atoms1, atoms2], format="extxyz")

    # 2. Convert to LMDB
    cv_xyz_to_lmdb([input_xyz], temp_lmdb)

    # 3. Convert back to XYZ
    cv_lmdb_to_xyz(temp_lmdb, reverse_xyz)

    # 4. Verify reversed XYZ
    assert Path(reverse_xyz).exists()
    reloaded = read(reverse_xyz, index=":")
    assert len(reloaded) == 2

    assert reloaded[0].get_chemical_formula() == "H2"
    assert reloaded[0].info["label"] == "test1"
    # Energy is usually in calc.results or info
    assert reloaded[0].calc.results["energy"] == -1.5

    assert reloaded[1].get_chemical_formula() == "O2"
    assert reloaded[1].info["label"] == "test2"
    assert reloaded[1].calc.results["energy"] == -2.5

    # Clean up
    for f in [input_xyz, temp_lmdb, reverse_xyz]:
        Path(f).unlink(missing_ok=True)


def test_convert_noble_gas_to_lmdb():
    """Test conversion using real data from data folder."""
    data_xyz = "data/structs_noble_gas_compounds.xyz"
    output_lmdb = "tests/noble_gas.aselmdb"

    if not Path(data_xyz).exists():
        import pytest

        pytest.skip(f"Data file {data_xyz} not found")

    # Clean up
    Path(output_lmdb).unlink(missing_ok=True)

    # 1. Read original
    original_atoms = read(data_xyz, index=":")

    # 2. Convert
    cv_xyz_to_lmdb([data_xyz], output_lmdb)

    # 3. Verify
    assert Path(output_lmdb).exists()
    with LMDBDatabase(output_lmdb, readonly=True) as db:
        assert len(db.ids) == len(original_atoms)

        for i, orig in enumerate(original_atoms):
            idx = i + 1
            loaded = db.get_atoms(idx)

            # Check basic structure
            assert len(loaded) == len(orig)
            assert np.allclose(loaded.positions, orig.positions)
            assert np.all(loaded.numbers == orig.numbers)

            # Check nested info
            for key, val in orig.info.items():
                # Some keys might be modified during db storage (e.g. pbc converted to array)
                if key == "pbc":
                    continue
                assert key in loaded.info
                if isinstance(val, (np.ndarray, list)):
                    assert np.allclose(loaded.info[key], val)
                else:
                    assert loaded.info[key] == val

            # Check arrays (e.g. forces)
            for key, val in orig.arrays.items():
                if key in ["numbers", "positions"]:
                    continue
                assert key in loaded.arrays
                assert np.allclose(loaded.arrays[key], val)

            # Check calculator
            if orig.calc is not None:
                assert loaded.calc is not None
                for key, val in orig.calc.results.items():
                    assert key in loaded.calc.results
                    assert np.allclose(loaded.calc.results[key], val)

    # Clean up
    Path(output_lmdb).unlink(missing_ok=True)


if __name__ == "__main__":
    test_convert_to_lmdb_aselmdb_format()
    test_convert_lmdb_to_xyz()
    test_convert_noble_gas_to_lmdb()
