import sys, subprocess
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.absolute()))
from tests.test_process_omol25 import create_mock_data

out_dir = Path("test_output_dir")
local_data_dir = Path("mock_s3_data")
test_data_source = Path("test_noble_gas_prefix.json")

# Cleanup
for d in [out_dir, local_data_dir]:
    if d.exists():
        for p in sorted(d.rglob('*'), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
        d.rmdir()

# Setup
Path(test_data_source).write_bytes(Path("data/noble_gas_compounds_prefix.json").read_bytes())
create_mock_data(local_data_dir, test_data_source)

cmd = [
    "mpirun", "--oversubscribe", "-n", "2", 
    sys.executable, "-m", "process_omol25.cli",
    "--data-source", str(test_data_source),
    "--output-dir", str(out_dir),
    "--local-dir", str(local_data_dir),
    "--mpi", "--log-level", "DEBUG"
]
print("Running command:", " ".join(cmd))
sys.stdout.flush()
subprocess.run(cmd)
print("Finished!")
