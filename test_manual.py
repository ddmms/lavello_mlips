import sys, os, subprocess
print("Running test...")
cmd = [
    "mpirun", "--oversubscribe", "-n", "2", 
    sys.executable, "-m", "process_omol25.cli",
    "--data-source", "data/noble_gas_compounds_prefix.json",
    "--output-dir", "out_tmp",
    "--sample-size", "5",
    "--mpi"
]
print("Running command:", " ".join(cmd))
sys.stdout.flush()
# run it synchronously
subprocess.run(cmd)
print("Finished!")
