import os
import shutil



def run_s2c(l1c_safe,l2a_out):
    """
    Run sen2cor subprocess
    :param l1c_safe: Path to SAFE folder
    :param l2a_out: Path to output directory for generated L2A products
    :return: Path to L2A SAFE
    """
    # L2A_Process is expected to be added to /bin/
    # After installing sen2cor run source Sen2Cor-02.09.00-Linux64/L2A_Bashrc
    # This should work in container and local env
    s2c_cmd = f"./Sen2Cor-02.09.00-Linux64/bin/L2A_Process {l1c_safe} --output_dir {l2a_out} --resolution 10"
    os.system(s2c_cmd)
    l2a_safe_folder = [os.path.join(l2a_out, fold) for fold in os.listdir(l2a_out) if fold.endswith('SAFE')][0]
    return l2a_safe_folder

def clean(folder):
    shutil.rmtree(folder)

def last_safe(safe_folder):
    """
    Get the deepest/last SAFE folder when there are many
    :param safe_folder: path to .SAFE folder
    :return: last path
    """
    tmp = safe_folder
    for root, dirs, files in os.walk(safe_folder):
        for dir in dirs:
            if dir.endswith('SAFE'):
                tmp = os.path.join(root,dir)
    return tmp