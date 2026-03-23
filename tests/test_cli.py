import sys
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from lavello_mlips.cli import main as cli_main

def test_cli_serial(tmp_path):
    """Test CLI orchestration in serial mode."""
    data_source = tmp_path / "train.json"
    data_source.write_text("{}")
    login_file = tmp_path / "login.json"
    login_file.write_text('{"access_key": "abc", "secret_key": "def"}')
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    test_args = [
        "lavello-mlips",
        "--data-source", str(data_source),
        "--login-file", str(login_file),
        "--output-dir", str(output_dir),
        "--sample-size", "10",
        "--batch-size", "5"
    ]

    with patch.object(sys, "argv", test_args), \
         patch("lavello_mlips.cli.OmolDataProcessor") as MockProcessor, \
         patch("lavello_mlips.cli.setup_logging") as mock_setup_logging:
        
        mock_instance = MockProcessor.return_value
        cli_main()

        # Verify OmolDataProcessor initialization
        MockProcessor.assert_called_once()
        args, rank, size, comm = MockProcessor.call_args[0]
        assert args.sample_size == 10
        assert args.batch_size == 5
        assert args.login_file == login_file
        assert rank == 0
        assert size == 1
        assert comm is None

        # Verify run_mpi was called
        mock_instance.run_mpi.assert_called_once()

        # Verify setup_logging was called
        mock_setup_logging.assert_called_once()

def test_cli_mpi(tmp_path):
    """Test CLI orchestration in MPI mode."""
    data_source = tmp_path / "train_mpi.json"
    data_source.write_text("{}")
    local_dir = tmp_path / "local_data"
    local_dir.mkdir()

    test_args = [
        "lavello-mlips",
        "--data-source", str(data_source),
        "--local-dir", str(local_dir),
        "--mpi",
        "--log-level", "DEBUG"
    ]

    # Mock MPI stuff
    mock_comm = MagicMock()
    mock_comm.Get_rank.return_value = 0
    mock_comm.Get_size.return_value = 4

    with patch.object(sys, "argv", test_args), \
         patch("lavello_mlips.cli.OmolDataProcessor") as MockProcessor, \
         patch("lavello_mlips.cli.mpi") as mock_mpi, \
         patch("lavello_mlips.cli.setup_logging") as mock_setup_logging:
        
        mock_mpi.COMM_WORLD = mock_comm
        cli_main()

        # Verify OmolDataProcessor initialization with MPI info
        MockProcessor.assert_called_once()
        args, rank, size, comm = MockProcessor.call_args[0]
        assert args.mpi is True
        assert rank == 0
        assert size == 4
        assert comm == mock_comm
        assert args.local_dir == local_dir

        # Verify logging level
        mock_setup_logging.assert_called_once()
        assert mock_setup_logging.call_args[1]["level"] == logging.DEBUG

def test_cli_error_no_credentials(tmp_path):
    """Test that CLI raises ValueError if no credentials provided."""
    data_source = tmp_path / "train_err.json"
    data_source.write_text("{}")

    test_args = [
        "lavello-mlips",
        "--data-source", str(data_source)
    ]

    with patch.object(sys, "argv", test_args), \
         patch("lavello_mlips.cli.OmolDataProcessor"), \
         patch("lavello_mlips.cli.setup_logging"):
        
        with pytest.raises(ValueError, match="--login-file is required"):
            cli_main()

def test_cli_worker_rank_no_logging(tmp_path):
    """Test that rank > 0 does not setup logging."""
    data_source = tmp_path / "train_worker.json"
    data_source.write_text("{}")
    local_dir = tmp_path / "local_data_worker"
    local_dir.mkdir()

    test_args = [
        "lavello-mlips",
        "--data-source", str(data_source),
        "--local-dir", str(local_dir),
        "--mpi"
    ]

    mock_comm = MagicMock()
    mock_comm.Get_rank.return_value = 1
    mock_comm.Get_size.return_value = 4

    with patch.object(sys, "argv", test_args), \
         patch("lavello_mlips.cli.OmolDataProcessor"), \
         patch("lavello_mlips.cli.mpi") as mock_mpi, \
         patch("lavello_mlips.cli.setup_logging") as mock_setup_logging:
        
        mock_mpi.COMM_WORLD = mock_comm
        cli_main()

        # setup_logging should NOT be called for rank > 0
        mock_setup_logging.assert_not_called()
