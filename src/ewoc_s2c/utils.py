""" EWoC Sen2Cor utils module"""
import glob
import logging
import os
import shutil
import signal
import sys
from contextlib import ContextDecorator
from pathlib import Path

import boto3.exceptions
import numpy as np
import rasterio
from eotile.eotile_module import main
from ewoc_dag.bucket.ewoc import EWOCARDBucket, EWOCAuxDataBucket
from ewoc_dag.cli_dem import get_dem_data
from ewoc_dag.utils import find_l2a_band, get_s2_prodname, raster_to_ard
from rasterio.merge import merge

logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = 900


def binary_scl(scl_file, raster_fn):
    """
    Convert L2A SCL file to binary cloud mask
    :param scl_file: Path to SCL file
    :param raster_fn: Output binary mask path
    """
    with rasterio.open(scl_file, "r") as src:
        scl = src.read(1)

    # Set the to-be-masked SCL values
    scl_mask_values = [0, 1, 3, 8, 9, 10, 11]

    # Set the nodata value in SCL
    scl_nodata_value = 0

    # Contruct the final binary 0-1-255 mask
    mask = np.zeros_like(scl)
    mask[scl == scl_nodata_value] = 255
    mask[~np.isin(scl, scl_mask_values)] = 1

    meta = src.meta.copy()
    meta["driver"] = "GTiff"
    dtype = rasterio.uint8
    meta["dtype"] = dtype
    meta["nodata"] = 255

    with rasterio.open(
        raster_fn,
        "w+",
        **meta,
        compress="deflate",
        tiled=True,
        blockxsize=512,
        blockysize=512,
    ) as out:
        out.write(mask.astype(rasterio.uint8), 1)


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
    input_file = str(Path(work_dir) / (prod_name + ".tif"))
    binary_scl(input_file, raster_cld)
    try:
        os.remove(input_file)
        os.remove(raster_cld + ".aux.xml")
    except FileNotFoundError:
        logger.info("Clean")


def l2a_to_ard(l2a_folder, work_dir, only_scl=False):
    """
    Convert an L2A product into EWoC ARD format
    :param l2a_folder: L2A SAFE folder
    :param work_dir: Output directory
    """
    if only_scl:
        bands = {
            "SCL": 20,
        }
    else:
        bands = {
            "B02": 10,
            "B03": 10,
            "B04": 10,
            "B08": 10,
            "B05": 20,
            "B06": 20,
            "B07": 20,
            "B11": 20,
            "B12": 20,
            "SCL": 20,
        }
    # Prepare ewoc folder name
    prod_name = get_s2_prodname(l2a_folder)
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
    ard_folder = os.path.join(folder_st, dir_name)
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    # Convert bands and SCL
    for band in bands:
        res = bands[band]
        band_path = find_l2a_band(l2a_folder, band, bands[band])
        band_name = os.path.split(band_path)[-1]
        band_name = band_name.replace(".jp2", ".tif").replace(f"_{str(res)}m", "")
        logger.info("Processing band " + band_name)
        out_name = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_{band}.tif"
        raster_fn = os.path.join(folder_st, dir_name, out_name)
        if band == "SCL":
            out_cld = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_MASK.tif"
            raster_cld = os.path.join(folder_st, dir_name, out_cld)
            binary_scl(band_path, raster_cld)
            logger.info("Done --> " + raster_cld)
            try:
                os.remove(raster_cld + ".aux.xml")
            except FileNotFoundError:
                logger.info("Clean")

        else:
            raster_to_ard(band_path, band, raster_fn)
            logger.info("Done --> " + raster_fn)
    return ard_folder


def set_logger(verbose_v):
    """
    Set the logger level
    :param loglevel:
    :return:
    """
    v_to_level = {"v": "INFO", "vv": "DEBUG"}
    loglevel = v_to_level[verbose_v]
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S"
    )


class TimeoutErrorSg(Exception):
    """
    Timeout Exception
    """

    pass


class TimeOut(ContextDecorator):
    """
    Time out decorator
    """

    def __init__(self, secs):
        self.seconds = secs

    def _handle_timeout(self, signum, frame):
        raise TimeoutErrorSg("Function call timed out")

    def __enter__(self):
        signal.signal(signal.SIGALRM, self._handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)


def run_s2c(
    l1c_safe,
    l2a_out,
    only_scl=False,
    bin_path="./Sen2Cor-02.09.00-Linux64/bin/L2A_Process",
):
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
        s2c_cmd = f"{bin_path} {l1c_safe} --output_dir {l2a_out} --sc_only"
    else:
        s2c_cmd = (
            f"{bin_path} {l1c_safe} --output_dir {l2a_out} --resolution 10 --debug"
        )
    os.system(s2c_cmd)
    # TODO: select folder using date and tile id from l1 id
    l2a_safe_folder = [
        os.path.join(l2a_out, fold)
        for fold in os.listdir(l2a_out)
        if fold.endswith("SAFE")
    ][0]
    return l2a_safe_folder


def clean(folder):
    """
    Delete folder recursively
    :param folder: Path to folder to be deleted
    :return: None
    """
    shutil.rmtree(folder)


def last_safe(safe_folder):
    """
    Get the deepest/last SAFE folder when there are many
    :param safe_folder: path to .SAFE folder
    :return: last path
    """
    tmp = safe_folder
    for root, dirs, _ in os.walk(safe_folder):
        for dir_ in dirs:
            if dir_.endswith("SAFE"):
                tmp = os.path.join(root, dir_)
    return tmp


def ewoc_s3_upload(local_path, ard_prd_prefix):
    """
    Upload file to the Cloud (S3 bucket)
    :param local_path: Path to the file to be uploaded
    :param ard_prd_prefix: Bucket prefix where store data
    :return: None
    """
    try:
        # Try to upload to s3 bucket,
        # you'll need to define some env vars needed for the s3 client
        # and destination path
        s3_bucket = EWOCARDBucket()

        s3_bucket.upload_ard_prd(local_path, ard_prd_prefix)
        # <!> Delete output folder after upload
        clean(local_path)
        logger.info(f"{local_path} cleared")
    except boto3.exceptions.S3UploadFailedError:
        logger.info("Could not upload output folder to s3, results saved locally")


def init_folder(folder_path):
    """
    Create some work folders, delete if existing
    :param folder_path: Path to folders location
    :return: None
    """
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
    """
    Crearte folders
    :param work_dir: folders location
    :return: None
    """
    out_dir_in = os.path.join(work_dir, "tmp_in")
    out_dir_proc = os.path.join(work_dir, "tmp_proc")
    init_folder(out_dir_in)
    init_folder(out_dir_proc)
    return out_dir_in, out_dir_proc


def custom_s2c_dem(dem_type, tile_id, tmp_dir):
    """
    Download and create a DEM mosaïc
    :param dem_type: DEM type (srtm or copdem)
    :param tile_id: MGRS tile id (ex 31TCJ Toulouse)
    :param tmp_dir: Output directory
    :return: list of links to the downloaded DEM files
    """
    # Clear the folder from tiles remaining from previous runs
    s2c_docker_dem_folder = f"/root/sen2cor/2.9/dem/{dem_type}"
    clean(s2c_docker_dem_folder)
    logger.info(f"/root/sen2cor/2.9/dem/{dem_type} --> clean (deleted)")
    # Create (back) the dem folder
    os.makedirs(s2c_docker_dem_folder)
    logger.info(f"/root/sen2cor/2.9/dem/{dem_type} --> created")
    # Download the dem files
    if dem_type=="srtm":
        get_dem_data(tile_id, Path(tmp_dir), dem_source="ewoc", dem_type=dem_type, dem_resolution="3s")
        raster_list = glob.glob(os.path.join(tmp_dir, "srtm3s", "*.tif"))
    elif dem_type=="copdem":
        get_dem_data(tile_id, Path(tmp_dir), dem_source="aws", dem_type=dem_type, dem_resolution="3s")
        raster_list = glob.glob(os.path.join(tmp_dir, "*.tif"))
    else:
        raise AttributeError("Attribute dem_type must be srtm or copdem")

    sources = []
    output_fn = os.path.join(tmp_dir, 'mosaic.tif')

    for raster_name in raster_list:
        src = rasterio.open(raster_name)
        sources.append(src)
    merge(sources, dst_path=output_fn, method="max")
    logger.info(f"Created mosaic {output_fn}")
    for src in sources:
        src.close()
    links = []
    for raster_name in raster_list:
        try:
            if dem_type=="copdem":
		raster_name = os.path.basename(raster_name).replace("_COG_", "_")
            os.symlink(output_fn, os.path.join(s2c_docker_dem_folder, raster_name))
            links.append(os.path.join(s2c_docker_dem_folder, raster_name))
        except OSError:
            logger.info("Symlink error: probably already exists")
    return links


def unlink(links):
    """
    Remove symlinks created
    :param links: List of links
    :return: None
    """
    for symlink in links:
        try:
            os.unlink(symlink)
            logger.info(f" -- [Ok] Unlinked {symlink}")
        except FileNotFoundError:
            logger.info(f"Cannot unlink {symlink}")
