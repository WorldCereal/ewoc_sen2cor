import os
import sys

l1c_safe = sys.argv[1]
l2a_out = sys.argv[2]
s2c_cmd = f"./Sen2Cor-02.09.00-Linux64/bin/L2A_Process {l1c_safe} --output_dir {l2a_out} --resolution 10"
os.system(s2c_cmd)
l2a_safe_folder = [os.path.join(l2a_out,fold) for fold in os.listdir(l2a_out) if fold.endswith('SAFE')][0]
print(l2a_safe_folder)
out_ard = f"dataship l2a_ard -f {l2a_safe_folder} -o {sys.argv[2]}_ard"
os.system(out_ard)
clean_cmd = f"rm -r {l2a_out}"
os.system(clean_cmd)
