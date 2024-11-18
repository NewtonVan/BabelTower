import os
import subprocess
from typing import Optional

def run_babel_tower(block: Optional[str] = None, virtgb: Optional[int] = None, physgb: Optional[int] = None, 
                     exmap: Optional[int] = None, batch: Optional[int] = None, runfor: Optional[int] = None, 
                     rndread: Optional[int] = None, threads: Optional[int] = None, datasize: Optional[int] = None) -> None:
    # Set environment variables if they are not None
    if block is not None:
        os.environ['BLOCK'] = block
    if virtgb is not None:
        os.environ['VIRTGB'] = str(virtgb)
    if physgb is not None:
        os.environ['PHYSGB'] = str(physgb)
    if exmap is not None:
        os.environ['EXMAP'] = str(exmap)
    if batch is not None:
        os.environ['BATCH'] = str(batch)
    if runfor is not None:
        os.environ['RUNFOR'] = str(runfor)
    if rndread is not None:
        os.environ['RNDREAD'] = str(rndread)
    if threads is not None:
        os.environ['THREADS'] = str(threads)
    if datasize is not None:
        os.environ['DATASIZE'] = str(datasize)
    
    # Construct the command
    command = ['./babel_tower']
    
    # Run the command
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Ensure the bench_result directory exists
    os.makedirs('bench_result', exist_ok=True)
    
    with open('bench_result/result', 'w') as result_file, open('bench_result/err', 'w') as err_file:
        for line in process.stdout:
            print(line, end='')  # Print to console
            result_file.write(line)  # Write to result file
        
        for line in process.stderr:
            print(line, end='')  # Print to console
            err_file.write(line)  # Write to error file
    
    process.wait()

if __name__ == "__main__":
    # Example usage
    run_babel_tower(
        block='/dev/sda',
        threads=4,
        datasize=2
    )