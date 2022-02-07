""" EWoC Sen2Cor utils module"""
import logging
import os
import shutil
import signal
import subprocess
import sys
from contextlib import ContextDecorator
from pathlib import Path
from typing import List, Optional, Tuple 

import boto3.exceptions
import numpy as np
import rasterio
from eotile.eotile_module import main
from ewoc_dag.bucket.ewoc import EWOCARDBucket, EWOCAuxDataBucket
from ewoc_dag.utils import find_l2a_band, get_s2_prodname, raster_to_ard
from rasterio.merge import merge

logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = 900


def binary_scl(scl_file: Path, raster_fn: Path):
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


def scl_to_ard(work_dir: Path, prod_name: str):
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
    folder_st = work_dir / "OPTICAL" / tile_id[:2] / tile_id[2] / tile_id[3:] / year / date.split("T")[0]
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    tmp_dir = folder_st / dir_name
    tmp_dir.mkdir(exist_ok=False, parents=True)

    out_cld = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_MASK.tif"
    raster_cld = folder_st / dir_name / out_cld
    input_file = work_dir / prod_name / ".tif"
    binary_scl(input_file, raster_cld)
    try:
        input_file.unlink()
        (raster_cld / ".aux.xml").unlink()
    except FileNotFoundError:
        logger.info("Clean")


def l2a_to_ard(l2a_folder: Path, work_dir: Path, only_scl: bool = False)-> Path:
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
    folder_st = work_dir / "OPTICAL" / tile_id[:2] / tile_id[2] / tile_id[3:] / year / date.split("T")[0]
    dir_name = f"{platform}_{processing_level}_{date}_{unique_id}_{tile_id}"
    tmp_dir = folder_st / dir_name
    ard_folder = folder_st / dir_name
    tmp_dir.mkdir(exist_ok=False, parents=True)

    # Convert bands and SCL
    for band in bands:
        res = bands[band]
        band_path = find_l2a_band(l2a_folder, band, bands[band])
        band_name = band_path.parts[-1]
        band_name = band_name.replace(".jp2", ".tif").replace(f"_{str(res)}m", "")
        logger.info("Processing band " + band_name)
        out_name = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_{band}.tif"
        raster_fn = folder_st / dir_name / out_name
        if band == "SCL":
            out_cld = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_MASK.tif"
            raster_cld = folder_st / dir_name / out_cld
            binary_scl(band_path, raster_cld)
            logger.info("Done --> " + str(raster_cld))
            try:
                (raster_cld / ".aux.xml").unlink()
            except FileNotFoundError:
                logger.info("Clean")

        else:
            raster_to_ard(band_path, band, raster_fn)
            logger.info("Done --> " + str(raster_fn))
    return ard_folder


def set_logger(verbose_v: str):
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

    def __init__(self, secs: float):
        self.seconds = secs

    def _handle_timeout(self, signum, frame):
        raise TimeoutErrorSg("Function call timed out")

    def __enter__(self):
        signal.signal(signal.SIGALRM, self._handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.alarm(0)


def run_s2c(
    l1c_safe: Path,
    l2a_out: Path,
    only_scl: bool = False,
    bin_path: str = "./Sen2Cor-02.09.00-Linux64/bin/L2A_Process",
)->Path:
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
    execute_cmd(s2c_cmd)
    # TODO: select folder using date and tile id from l1 id
    l2a_safe_folder = [
        l2a_out / fold
        for fold in os.listdir(l2a_out)
        if fold.endswith("SAFE")
    ][0]
    return l2a_safe_folder


def clean(folder: Path):
    """
    Delete folder recursively
    :param folder: Path to folder to be deleted
    :return: None
    """
    shutil.rmtree(folder)


def last_safe(safe_folder: Path)->Path:
    """
    Get the deepest/last SAFE folder when there are many
    :param safe_folder: path to .SAFE folder
    :return: last path
    """
    tmp = safe_folder
    for root, dirs, _ in walk(safe_folder):
        for dir_ in dirs:
            if dir_.endswith("SAFE"):
                tmp = root / dir
    return tmp


def ewoc_s3_upload(local_path: Path, ard_prd_prefix: str):
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


def init_folder(folder_path: Path):
    """
    Create some work folders, delete if existing
    :param folder_path: Path to folders location
    :return: None
    """
    if folder_path.is_dir():
        logger.info(f"Found {folder_path} -- start reset")
        clean(folder_path)
        logger.info(f"Cleared {folder_path}")
        folder_path.mkdir(exist_ok=False, parents=True)
        logger.info(f"Created new folder {folder_path}")
    else:
        folder_path.mkdir(exist_ok=False, parents=True)
        logger.info(f"Created new folder {folder_path}")


def make_tmp_dirs(work_dir: Path)->Tuple[Path, Path]:
    """
    Crearte folders
    :param work_dir: folders location
    :return: None
    """
    out_dir_in = work_dir / "tmp_in"
    out_dir_proc = work_dir / "tmp_proc"
    init_folder(out_dir_in)
    init_folder(out_dir_proc)
    return out_dir_in, out_dir_proc


def custom_s2c_dem(tile_id: str, tmp_dir: Path)->List:
    """
    Download and create an srtm mosaïc
    :param tile_id: MGRS tile id (ex 31TCJ Toulouse)
    :param tmp_dir: Output directory
    :return: list of links to the downloaded DEM files
    """
    srt_90 = main(tile_id, no_l8=True, no_s2=True, srtm5x5=True, overlap=True)
    srt_90 = srt_90[-1]
    srtm_ids = list(srt_90["id"])
    # Clear the srtm folder from tiles remaining from previous runs
    s2c_docker_srtm_folder = Path("/root/sen2cor/2.9/dem/srtm")
    clean(s2c_docker_srtm_folder)
    logger.info("/root/sen2cor/2.9/dem/srtm --> clean (deleted)")
    # Create (back) the srtm folder
    s2c_docker_srtm_folder.mkdir(exist_ok=False, parents=True)
    logger.info("/root/sen2cor/2.9/dem/srtm --> created")
    # download the zip files
    bucket = EWOCAuxDataBucket()
    bucket.download_srtm3s_tiles(srtm_ids, tmp_dir)

    sources = []
    output_fn = tmp_dir / f'mosaic_{"_".join(srtm_ids)}.tif'

    for srtm_id in srtm_ids:
        raster_name = tmp_dir / "srtm3s", srtm_id + ".tif"
        src = rasterio.open(raster_name)
        sources.append(src)
    merge(sources, dst_path=output_fn, method="max")
    logger.info(f"Created mosaic {output_fn}")
    for src in sources:
        src.close()
    links = []
    for tile in srtm_ids:
        try:
            (s2c_docker_srtm_folder / tile / ".tif").symlink_to(output_fn)
            links.append(s2c_docker_srtm_folder / tile / ".tif")
        except OSError:
            logger.info("Symlink error: probably already exists")
    return links


def unlink(links: List):
    """
    Remove symlinks created
    :param links: List of links
    :return: None
    """
    for symlink in links:
        try:
            symlink.unlink()
            logger.info(f" -- [Ok] Unlinked {symlink}")
        except FileNotFoundError:
            logger.info(f"Cannot unlink {symlink}")


def walk(path: Path): 
    for p in path.iterdir(): 
        if p.is_dir(): 
            yield from walk(p)
            continue
        yield p.resolve()


def execute_cmd(cmd: str):
    """
    Execute the given cmd.
    :param cmd: The command and its parameters to execute
    """
    logger.debug("Launching command: %s", cmd)
    try:
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    except OSError as err:
        logger.error('An error occurred while running command \'%s\'', cmd, exc_info=True)
        return err.errno, str(err), err.strerror 
