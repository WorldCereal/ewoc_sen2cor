import shutil
import signal
import sys
from contextlib import ContextDecorator

import rasterio
from dataship.dag.s3man import *
from dataship.dag.utils import get_product_by_id
from eotile.eotile_module import main
from rasterio.merge import merge
from dataship.dag.utils import binary_scl
from pathlib import Path
logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = 900


def scl_to_ard(work_dir, prod_name):
    """
    Convert the SCL L2A product into EWoC ARD format
    :param work_dir: Output directory
    :param prod_name: The name of the tif product
    """
    # Prepare ewoc folder name
    product_id = prod_name
    platform = product_id.split("_")[0]
    processing_level = product_id.split("_")[1]
    date = product_id.split("_")[2]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = product_id.split("_")[5][1:]
    atcor_algo = "L2A"
    unique_id = "".join(product_id.split("_")[3:6])
    folder_st = os.path.join(
        work_dir,
        "OPTICAL",
        tile_id[:2],
        tile_id[2],
        tile_id[3:],
        year,
        date.split("T")[0],
    )
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    tmp_dir = os.path.join(folder_st, dir_name)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    out_cld = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_MASK.tif"
    raster_cld = os.path.join(folder_st, dir_name, out_cld)
    input_file = str(Path(work_dir)/(prod_name+'.tif'))
    binary_scl(input_file, raster_cld)
    try:
        os.remove(input_file)
        os.remove(raster_cld + ".aux.xml")
    except:
        logger.info("Clean")


def set_logger(verbose_v):
    """
    Set the logger level
    :param verbose_v: v for info, vv for debug (default to warning)
    :type verbose_v: str
    """
    v_to_level = {"v": "INFO", "vv": "DEBUG"}
    loglevel = v_to_level[verbose_v]
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


class TimeoutError(Exception):
    pass


class timeout(ContextDecorator):
    def __init__(self, secs):
        self.seconds = secs

    def _handle_timeout(self, signum, frame):
        raise TimeoutError("Function call timed out")

    def __enter__(self):
        signal.signal(signal.SIGALRM, self._handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)


def run_s2c(l1c_safe, l2a_out, only_scl):
    """
    Run sen2cor subprocess
    :param l1c_safe: Path to SAFE folder
    :param l2a_out: Path to output directory for generated L2A products
    :return: Path to L2A SAFE
    """
    # L2A_Process is expected to be added to /bin/
    # After installing sen2cor run source Sen2Cor-02.09.00-Linux64/L2A_Bashrc
    # This should work in container and local env
    if only_scl:
        s2c_cmd = f"./Sen2Cor-02.09.00-Linux64/bin/L2A_Process {l1c_safe} --output_dir {l2a_out} --sc_only"
    else:
        s2c_cmd = f"./Sen2Cor-02.09.00-Linux64/bin/L2A_Process {l1c_safe} --output_dir {l2a_out} --resolution 10"
    os.system(s2c_cmd)
    # TODO instead of getting the first element of this list, select folder using date and tile id from l1 id
    l2a_safe_folder = [
        os.path.join(l2a_out, fold)
        for fold in os.listdir(l2a_out)
        if fold.endswith("SAFE")
    ][0]
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
            if dir.endswith("SAFE"):
                tmp = os.path.join(root, dir)
    return tmp


def get_var_env(var_name):
    return os.getenv(var_name)


def ewoc_s3_upload(local_path):
    try:
        # Try to upload to s3 bucket, you'll need to define some env vars needed for the s3 client and destination path
        s3c = get_s3_client()
        bucket = get_var_env("BUCKET")
        s3_path = get_var_env("DEST_PREFIX")
        recursive_upload_dir_to_s3(
            s3_client=s3c, local_path=local_path, s3_path=s3_path, bucketname=bucket
        )
        # <!> Delete output folder after upload
        clean(local_path)
        logger.info(f"{local_path} cleared")
    except:
        logger.info("Could not upload output folder to s3, results saved locally")


def init_folder(folder_path):
    if os.path.exists(folder_path):
        logger.info(f"Found {folder_path} -- start reset")
        clean(folder_path)
        logger.info(f"Cleared {folder_path}")
        os.makedirs(folder_path)
        logger.info(f"Created new folder {folder_path}")
    else:
        os.makedirs(folder_path)
        logger.info(f"Created new folder {folder_path}")


def make_tmp_dirs(work_dir):
    out_dir_in = os.path.join(work_dir, "tmp_in")
    out_dir_proc = os.path.join(work_dir, "tmp_proc")
    init_folder(out_dir_in)
    init_folder(out_dir_proc)
    return out_dir_in, out_dir_proc


def custom_s2c_dem(tile_id, tmp_dir):
    srt_90 = main(tile_id, no_l8=True, no_s2=True, srtm5x5=True, overlap=True)
    srt_90 = srt_90[-1]
    srtm_ids = list(srt_90["id"])
    bucket = "world-cereal"
    srtm_list = []
    srtm_tiles = []
    # Clear the srtm folder from tiles remaining from previous runs
    s2c_docker_srtm_folder = "/root/sen2cor/2.9/dem/srtm"
    clean(s2c_docker_srtm_folder)
    logger.info("/root/sen2cor/2.9/dem/srtm --> clean (deleted)")
    # Create (back) the srtm folder
    os.makedirs(s2c_docker_srtm_folder)
    logger.info("/root/sen2cor/2.9/dem/srtm --> created")
    # download the zip files
    for srtm_id in srtm_ids:
        key = os.path.join("srtm90", srtm_id + ".zip")
        outzip = os.path.join(tmp_dir, srtm_id + ".zip")
        download_s3file(key, outzip, bucket)
        outfold = outzip.replace(".zip", "")
        with zipfile.ZipFile(outzip, "r") as zip_ref:
            zip_ref.extractall(outfold)
        os.remove(outzip)
        srtm_list.append(os.path.join(tmp_dir, srtm_id, srtm_id + ".tif"))
        srtm_tiles.append(srtm_id)
    sources = []
    output_fn = os.path.join(tmp_dir, f'mosaic_{"_".join(srtm_ids)}.tif')
    for raster in srtm_list:
        src = rasterio.open(raster)
        sources.append(src)
    merge(sources, dst_path=output_fn, method="max")
    logger.info(f"Created mosaic {output_fn}")
    for src in sources:
        src.close()
    links = []
    for tile in srtm_tiles:
        try:
            os.symlink(output_fn, os.path.join(s2c_docker_srtm_folder, tile + ".tif"))
            links.append(os.path.join(s2c_docker_srtm_folder, tile + ".tif"))
        except OSError:
            logger.info("Symlink error: probably already exists")
    return links


def unlink(links):
    for symlink in links:
        try:
            os.unlink(symlink)
            logger.info(f" -- [Ok] Unlinked {symlink}")
        except:
            logger.info(f"Cannot unlink {symlink}")


def robust_get_by_id(pid, out_dir, provider):
    """
    Get product by id using multiple strategies
    :param pid: Sentinel-2 product id
    :param out_dir: Output directory where the SAFE folder will be downloaded
    """
    if provider.lower() == "creodias_eodata":
        pid = pid + ".SAFE"
        download_s2_prd_from_creodias(pid, Path(out_dir))
    elif provider.lower() == "creodias_finder":
        get_product_by_id(pid, out_dir)



