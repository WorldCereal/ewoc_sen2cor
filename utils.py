import os
import shutil
import zipfile

from dataship.dag.s3man import *
from eotile.eotile_module import main
import rasterio
from rasterio.merge import merge


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
    # TODO instead of getting the first element of this list, select folder using date and tile id from l1 id
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

def get_var_env(var_name):
    return os.getenv(var_name)

def ewoc_s3_upload():
    try:
        # Try to upload to s3 bucket, you'll need to define some env vars needed for the s3 client and destination path
        s3c = get_s3_client()
        bucket = get_var_env("BUCKET")
        local_path = get_var_env("S2C_LP")
        s3_path = get_var_env("DEST_PREFIX")
        recursive_upload_dir_to_s3(s3_client=s3c,local_path=local_path,s3_path=s3_path,bucketname=bucket)
        # <!> Delete output folder after upload
        clean(local_path)
    except:
        print("Could not upload output folder to s3, results saved locally")

def make_tmp_dirs(work_dir):
    out_dir_in = os.path.join(work_dir, "tmp_in")
    out_dir_proc = os.path.join(work_dir, "tmp_proc")
    if not os.path.exists(out_dir_in):
        os.makedirs(out_dir_in)
    if not os.path.exists(out_dir_proc):
        os.makedirs(out_dir_proc)
    return out_dir_in,out_dir_proc

def custom_s2c_dem(tile_id,tmp_dir):
    srt_90 = main(tile_id,no_l8=True, no_s2=True, srtm5x5=True)
    srt_90 = srt_90[-1]
    srtm_ids = list(srt_90['id'])
    bucket = "world-cereal"
    srtm_list=[]
    srtm_tiles=[]
    # download the zip files
    for srtm_id in srtm_ids:
        key = os.path.join("srtm90",srtm_id+".zip")
        outzip = os.path.join(tmp_dir,srtm_id+".zip")
        download_s3file(key,outzip,bucket)
        outfold = outzip.replace('.zip','')
        with zipfile.ZipFile(outzip, 'r') as zip_ref:
            zip_ref.extractall(outfold)
        os.remove(outzip)
        srtm_list.append(os.path.join(tmp_dir,srtm_id,srtm_id+'.tif'))
        srtm_tiles.append(srtm_id)
    sources = []
    output_fn = os.path.join(tmp_dir,'mosaic.tif')
    for raster in srtm_list:
        src = rasterio.open(raster)
        sources.append(src)
    merge(sources,dst_path=output_fn,method='max')
    for src in sources:
        src.close()
    s2c_docker_srtm_folder = "/root/sen2cor/2.9/dem/srtm"
    for tile in srtm_tiles:
        os.symlink(output_fn,os.path.join(s2c_docker_srtm_folder,tile+'.tif'))



