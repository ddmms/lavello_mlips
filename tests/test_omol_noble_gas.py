import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from ase.io import read
from lavello_mlips.process_omol25 import OmolDataProcessor

def test_omol_processor_noble_gas(tmp_path):
    """Test OmolDataProcessor using real noble gas data as ground truth."""
    # Paths
    ground_truth_xyz = Path("data/structs_noble_gas_compounds.xyz")
    prefix_json = Path("data/noble_gas_compounds_prefix.json")
    
    if not (ground_truth_xyz.exists() and prefix_json.exists()):
        pytest.skip("Noble gas data files not found")

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    
    # 1. Load ground truth and map prefixes to atoms
    all_ground_truth = read(str(ground_truth_xyz), index=":")
    prefix_to_atoms = {at.info["argonne_rel"]: at for at in all_ground_truth}
    
    # 2. Setup OmolDataProcessor
    # Mocking argparse.Namespace
    class MockArgs:
        def __init__(self):
            self.data_source = prefix_json
            self.output_dir = output_dir
            self.bucket = "mock-bucket"
            self.local_dir = "mock-local-dir"
            self.login_file = None
            self.group_name = "test_noble_gas"
            self.sample_size = -1
            self.batch_size = 100
            self.start_index = 0
            self.restart = False
            self.mpi = False
            self.memory_threshold_gb = 10.0

    args = MockArgs()
    
    # We mock S3DataProcessor.get_s3_client to return None (local mode)
    with patch("lavello_mlips.process_omol25.OmolDataProcessor.get_s3_client", return_value=None):
        processor = OmolDataProcessor(args, rank=0, size=1, comm=None)
        
        # 3. Mock process_single to return pre-mapped atoms from ground truth
        def mock_process_single(idx, s3_client=None):
            x = processor.prefixes[idx]
            if x in prefix_to_atoms:
                atoms = prefix_to_atoms[x].copy()
                # OmolDataProcessor expects a rec dict as well
                rec = atoms.info.copy()
                # Ensure geometry related info is exactly as in ground truth
                # sha1, cog, com, cnc are already in info from read()
                return rec, atoms, x
            return None

        with patch.object(processor, "process_single", side_effect=mock_process_single):
            # 4. Run serial processing
            processor.run_serial()
            
        # 5. Verify outputs
        # OmolDataProcessor derives group_name from filename stem:
        # noble_gas_compounds_prefix -> noble_gas_compounds
        final_parquet = output_dir / "props_noble_gas_compounds.parquet"
        final_xyz = output_dir / "structs_noble_gas_compounds.xyz"
        
        assert final_parquet.exists()
        assert final_xyz.exists()
        
        # Check Parquet content
        df = pd.read_parquet(final_parquet)
        assert len(df) == len(all_ground_truth)
        assert set(df["argonne_rel"]) == set(prefix_to_atoms.keys())
        
        # Check XYZ content
        processed_atoms = read(str(final_xyz), index=":")
        assert len(processed_atoms) == len(all_ground_truth)
        
        # Verify specific structure (e.g., first one)
        # Sort both by argonne_rel for comparison
        all_ground_truth_sorted = sorted(all_ground_truth, key=lambda at: at.info["argonne_rel"])
        processed_atoms_sorted = sorted(processed_atoms, key=lambda at: at.info["argonne_rel"])
        
        for gt, pr in zip(all_ground_truth_sorted, processed_atoms_sorted):
            assert gt.info["argonne_rel"] == pr.info["argonne_rel"]
            assert len(gt) == len(pr)
            assert np.allclose(gt.positions, pr.positions)
            assert np.all(gt.numbers == pr.numbers)
            # Verify a few key properties
            for key in ["energy", "total_charge_e", "multiplicity"]:
                if key in gt.info:
                    assert pr.info[key] == gt.info[key]

    print("OmolDataProcessor noble gas test passed!")
