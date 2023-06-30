""" EWoC Sen2Cor utils module"""
from datetime import datetime
import glob
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import List, Tuple, Generator
import uuid
import xml.etree.ElementTree as ET

import boto3.exceptions
from ewoc_dag.bucket.ewoc import EWOCARDBucket
from ewoc_dag.cli_dem import get_dem_data
from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo
from ewoc_dag.srtm_dag import get_srtm3s_ids
import lxml.etree as ET
from nptyping import NDArray
import numpy as np
import rasterio
from rasterio.merge import merge

from ewoc_s2c import __version__

logger = logging.getLogger(__name__)


def binary_scl(scl_file: Path, raster_fn: Path) -> None:
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
        # Modify output metadata
        out.update_tags(TIFFTAG_DATETIME=str(datetime.now()))
        out.update_tags(TIFFTAG_IMAGEDESCRIPTION="EWoC Sentinel-2 ARD")
        out.update_tags(TIFFTAG_SOFTWARE="EWoC S2 Processor " + str(__version__))

        out.write(mask.astype(rasterio.uint8), 1)


def scl_to_ard(work_dir: Path, prod_name: str) -> None:
    """
    Convert the SCL L2A product into EWoC ARD format
    :param work_dir: Output directory
    :param prod_name: The name of the tif product
    """
    # Prepare ewoc folder name
    product_id = prod_name
    platform = product_id.split("_")[0]
    date = product_id.split("_")[2]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = product_id.split("_")[5][1:]
    atcor_algo = "L2A"
    unique_id = "".join(product_id.split("_")[3:6])
    folder_st = (
        work_dir
        / "OPTICAL"
        / tile_id[:2]
        / tile_id[2]
        / tile_id[3:]
        / year
        / date.split("T")[0]
    )
    dir_name = f"{platform}_MSIL2A_{date}_{unique_id}_{tile_id}"
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


def retrieve_offset_from_meta(meta_xml_file: str, band_id: str) -> int:
    tree = ET.parse(meta_xml_file)
    root = tree.getroot()
    offset_band = root.find(f'.//BOA_ADD_OFFSET[@band_id="{band_id}"]').text
    offset_band = int(offset_band)
    return offset_band


def apply_offset(
    raster_band: NDArray[int], meta_xml_file: str, band_id: str
) -> NDArray[int]:
    # Read metadata
    offset_band = retrieve_offset_from_meta(meta_xml_file, band_id)
    # Apply offset
    logger.info("For band %s, offset is %s", band_id, offset_band)
    raster_band = raster_band + offset_band
    return raster_band


def l2a_to_ard(
    l2a_folder: Path, work_dir: Path, pid: str, provider: str, only_scl: bool = False
) -> Path:
    """
    Convert an L2A product into EWoC ARD format
    :param only_scl:
    :param pid:
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
    prod_name = pid.replace(".SAFE", "")
    product_id = prod_name
    platform = product_id.split("_")[0]
    date = product_id.split("_")[2]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = product_id.split("_")[5][1:]
    atcor_algo = "L2A"
    unique_id = "".join(product_id.split("_")[3:6])
    folder_st = (
        work_dir
        / "OPTICAL"
        / tile_id[:2]
        / tile_id[2]
        / tile_id[3:]
        / year
        / date.split("T")[0]
    )
    dir_name = f"{platform}_MSIL2A_{date}_{unique_id}_{tile_id}"
    tmp_dir = folder_st / dir_name
    ard_folder = folder_st / dir_name
    tmp_dir.mkdir(exist_ok=False, parents=True)

    # Convert bands and SCL
    for band in bands:
        res = bands[band]
        if provider == "aws_sng" and S2PrdIdInfo.is_l2a(pid):
            band_path = find_l2a_band_sng(l2a_folder, band, res)
        else:
            band_path = find_l2a_band(l2a_folder, band, res)
        band_name = band_path.parts[-1]
        band_name = band_name.replace(".jp2", ".tif").replace(f"_{str(res)}m", "")
        logger.info("Processing band %s", band_name)
        out_name = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_{band}.tif"
        raster_fn = folder_st / dir_name / out_name
        if band == "SCL":
            out_cld = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_MASK.tif"
            raster_cld = folder_st / dir_name / out_cld
            binary_scl(band_path, raster_cld)
            logger.info("Done --> %s", str(raster_cld))
            try:
                (raster_cld.with_suffix(".aux.xml")).unlink()
            except FileNotFoundError:
                logger.info("Clean")

        else:
            raster_to_ard(
                band_path, band, raster_fn, pid=product_id, data_source=provider
            )
            logger.info("Done --> %s", str(raster_fn))
    return ard_folder


def l2a_to_ard_aws_cog(
    l2a_folder: Path,
    work_dir: Path,
    provider: str,
    only_scl: bool = False,
) -> Path:
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
    prod_name = l2a_folder.name
    product_id = prod_name
    platform = product_id.split("_")[0]
    date = product_id.split("_")[2]
    year = date[:4]
    # Get tile id , remove the T in the beginning
    tile_id = product_id.split("_")[5][1:]
    atcor_algo = "L2A"
    unique_id = "".join(product_id.split("_")[3:6])
    folder_st = (
        work_dir
        / "OPTICAL"
        / tile_id[:2]
        / tile_id[2]
        / tile_id[3:]
        / year
        / date.split("T")[0]
    )
    dir_name = f"{platform}_MSIL2A_{date}_{unique_id}_{tile_id}"
    tmp_dir = folder_st / dir_name
    ard_folder = folder_st / dir_name
    tmp_dir.mkdir(exist_ok=False, parents=True)

    # Convert bands and SCL
    for band in bands:
        band_path = l2a_folder / f"{band}.tif"
        band_name = band_path.name
        logger.info("Processing band %s", band_name)
        out_name = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_{band}.tif"
        raster_fn = folder_st / dir_name / out_name
        if band == "SCL":
            out_cld = f"{platform}_{atcor_algo}_{date}_{unique_id}_{tile_id}_MASK.tif"
            raster_cld = folder_st / dir_name / out_cld
            binary_scl(band_path, raster_cld)
            logger.info("Done --> %s", str(raster_cld))
            try:
                (raster_cld.with_suffix(".aux.xml")).unlink()
            except FileNotFoundError:
                logger.info("Clean")

        else:
            raster_to_ard(
                band_path, band, raster_fn, pid=product_id, data_source=provider
            )
            logger.info("Done --> %s", str(raster_fn))
    return ard_folder


def get_s2_prodname(safe_path: Path) -> str:
    """
    Get Sentinel-2 product name
    :param safe_path: Path to SAFE folder
    :type safe_path: str
    :return: Product name
    :rtype: str
    """
    safe_split = str(safe_path).split("/")
    prodname = [item for item in safe_split if ".SAFE" in item][0]
    prodname = prodname.replace(".SAFE", "")
    return prodname


def raster_to_ard(
    raster_path: Path, band_num: str, raster_fn: Path, data_source: str, pid: str
) -> None:
    """
    Read raster and update internals to fit ewoc ard specs
    :param raster_path: Path to raster file
    :param band_num: Band number, B02 for example
    :param raster_fn: Output raster path
    :param data_source: source of the Sentinel-2 data
    :param pid: Sentinel-2 product id
    """

    band_id = {
        "B01": 0,
        "B02": 1,
        "B03": 2,
        "B04": 3,
        "B05": 4,
        "B06": 5,
        "B07": 6,
        "B08": 7,
        "B08A": 8,
        "B09": 9,
        "B10": 10,
        "B11": 11,
        "B12": 12,
    }

    with rasterio.Env(GDAL_CACHEMAX=2048):
        with rasterio.open(raster_path, "r") as src:
            raster_array = src.read()

            if (
                S2PrdIdInfo(pid).datatake_sensing_start_time.date()
                > datetime(2022, 1, 25).date()
                and S2PrdIdInfo(pid).pdgs_processing_baseline_number != "0400"
            ):
                logger.warning(
                    "Need to handle processing baselines after 0400 and check if an offset has to be applied"
                )

            if (
                S2PrdIdInfo(pid).pdgs_processing_baseline_number == "0400"
                and data_source == "aws_sng"
            ):
                logger.info(
                    f"Baseline is {S2PrdIdInfo(pid).pdgs_processing_baseline_number} and provider is {data_source}"
                )
                meta_xml_file = raster_path.parents[2] / "product/metadata.xml"
                raster_array = apply_offset(
                    raster_array, str(meta_xml_file), str(band_id[band_num])
                )

            meta = src.meta.copy()
    meta["driver"] = "GTiff"
    meta["nodata"] = 0
    bands_10m = ["B02", "B03", "B04", "B08"]
    blocksize = 512
    if band_num in bands_10m:
        blocksize = 1024
    with rasterio.open(
        raster_fn,
        "w+",
        **meta,
        tiled=True,
        compress="deflate",
        blockxsize=blocksize,
        blockysize=blocksize,
    ) as out:
        # Modify output metadata
        out.update_tags(TIFFTAG_DATETIME=str(datetime.now()))
        out.update_tags(TIFFTAG_IMAGEDESCRIPTION="EWoC Sentinel-2 ARD")
        out.update_tags(TIFFTAG_SOFTWARE="EWoC S2 Processor " + str(__version__))
        out.update_tags(DATASOURCE=f"S2 data source: {data_source}")
        out.update_tags(PRODUCTID=f"S2 product id: {pid}")

        out.write(raster_array)


def find_l2a_band(l2a_folder: Path, band_num: str, res: int) -> Path:
    """
    Find L2A band at specific resolution
    :param l2a_folder: L2A product folder
    :param band_num: BXX/AOT/SCL/...
    :param res: resolution (10/20/60)
    :return: path to band
    """
    # band_path = None
    id_img = f"{band_num}_{str(res)}m.jp2"
    for file in walk(l2a_folder):
        if str(file).endswith(id_img):
            band_path = file
    return band_path


def find_l2a_band_sng(l2a_folder: Path, band_num: str, res: int) -> Path:
    """
    Find L2A band at specific resolution
    :param l2a_folder: L2A product folder
    :param band_num: BXX/AOT/SCL/...
    :param res: resolution (10/20/60)
    :return: path to band
    """
    # band_path = None
    id_img = f"{band_num}.jp2"
    res_img = f"R{res}m"
    for file in walk(l2a_folder):
        fold_img = file.parts[-2]
        if str(file).endswith(id_img) and res_img == fold_img:
            band_path = file
    return band_path


def set_logger(verbose_v: str) -> None:
    """
    Set the logger level
    :param loglevel:
    :return:
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    v_to_level = {"v": "INFO", "vv": "DEBUG"}
    if verbose_v is None:
        loglevel = "ERROR"
        logging.basicConfig(
            level=loglevel,
            stream=sys.stdout,
            format=logformat,
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        set_sen2cor_log(loglevel)
    else:
        loglevel = v_to_level[verbose_v]
        logging.basicConfig(
            level=loglevel,
            stream=sys.stdout,
            format=logformat,
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        set_sen2cor_log(loglevel)


def run_s2c(
    l1c_safe: Path,
    l2a_out: Path,
    only_scl: bool = False,
    bin_path: str = "./Sen2Cor-02.09.00-Linux64/bin/L2A_Process",
) -> Path:
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
    try:
        execute_cmd(s2c_cmd)
    except RuntimeError:
        logger.error("Sen2cor execution error")
        sys.exit(1)
    # TODO: select folder using date and tile id from l1 id
    l2a_safe_folder = [
        l2a_out / fold for fold in os.listdir(l2a_out) if fold.endswith("SAFE")
    ][0]
    return l2a_safe_folder


def clean(folder: Path) -> None:
    """
    Delete folder recursively
    :param folder: Path to folder to be deleted
    :return: None
    """
    shutil.rmtree(folder)


def last_safe(safe_folder) -> str:
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


def ewoc_s3_upload(local_path: Path, ard_prd_prefix: str) -> None:
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
        nb_prd_pr, __unused, up_dir_pr = EWOCARDBucket().upload_ard_prd(
            local_path, ard_prd_prefix
        )
        # This print is made on purpose (not debug) :)
        print(f"Uploaded {nb_prd_pr} tif files to bucket | {up_dir_pr}")
        # <!> Delete output folder after upload
        clean(local_path)
        logger.info("%s cleared", local_path)
    except boto3.exceptions.S3UploadFailedError:
        logger.info("Could not upload output folder to s3, results saved locally")


def init_folder(folder_path: Path) -> None:
    """
    Create some work folders, delete if existing
    :param folder_path: Path to folders location
    :return: None
    """
    if folder_path.is_dir():
        logger.info("Found %s -- start reset", folder_path)
        clean(folder_path)
        logger.info("Cleared %s", folder_path)
        folder_path.mkdir(exist_ok=False, parents=True)
        logger.info("Created new folder %s", folder_path)
    else:
        folder_path.mkdir(exist_ok=False, parents=True)
        logger.info("Created new folder %s", folder_path)


def make_tmp_dirs(work_dir: Path) -> Tuple[Path, Path]:
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


def custom_s2c_dem(dem_type: str, tile_id: str) -> Tuple[Path, List]:
    """
    Download and create a DEM mosaïc
    :param dem_type: DEM type (srtm or copdem)
    :param tile_id: MGRS tile id (ex 31TCJ Toulouse)
    :return: DEM temporary directory and list of links to the downloaded DEM files
    """
    # Generate temporary folder
    dem_tmp_dir = Path("/work/SEN2TEST/DEM/")
    if dem_tmp_dir.exists():
        shutil.rmtree(dem_tmp_dir)
    dem_tmp_dir.mkdir(exist_ok=False, parents=True)
    # Clear the folder from tiles remaining from previous runs
    s2c_docker_dem_folder = f"/root/sen2cor/2.9/dem/{dem_type}"
    s2c_docker_dem_path = Path(f"/root/sen2cor/2.9/dem/{dem_type}")
    if s2c_docker_dem_path.exists():
        clean(s2c_docker_dem_path)
        logger.info("/root/sen2cor/2.9/dem/%s --> clean (deleted)", dem_type)
    # Create (back) the dem folder
    s2c_docker_dem_path.mkdir(parents=True)
    logger.info("/root/sen2cor/2.9/dem/%s --> created", dem_type)
    # Download the dem files
    if dem_type == "srtm":
        get_dem_data(
            tile_id,
            Path(dem_tmp_dir),
            dem_source="ewoc",
            dem_type=dem_type,
            dem_resolution="3s",
        )
        raster_list = glob.glob(os.path.join(dem_tmp_dir, "srtm3s", "*.tif"))
    elif dem_type == "copdem":
        get_dem_data(
            tile_id,
            Path(dem_tmp_dir),
            dem_source="aws",
            dem_type=dem_type,
            dem_resolution="3s",
        )
        raster_list = glob.glob(os.path.join(dem_tmp_dir, "*.tif"))
    else:
        raise AttributeError("Attribute dem_type must be srtm or copdem")

    sources = []
    uid = uuid.uuid4()
    output_fn = os.path.join(dem_tmp_dir, f"mosaic_{uid}.tif")

    for raster_name in raster_list:
        src = rasterio.open(raster_name)
        sources.append(src)
    merge(sources, dst_path=output_fn, method="max")
    logger.info("Created mosaic %s", output_fn)
    for src in sources:
        src.close()
    links = []

    # Artificially change copdem filenames to srtm filenames
    # to run sen2cor 2.9 with copdem
    if dem_type == "copdem":
        raster_list = get_srtm3s_ids(tile_id)

    for raster_name in raster_list:
        try:
            if dem_type == "copdem":
                raster_name = raster_name + ".tif"
            # raster_name = os.path.basename(raster_name).replace("_COG_", "_")
            if dem_type == "srtm":
                raster_name = os.path.basename(raster_name)
            os.symlink(output_fn, os.path.join(s2c_docker_dem_folder, raster_name))
            links.append(Path(os.path.join(s2c_docker_dem_folder, raster_name)))
        except OSError:
            logger.info("Symlink error: probably already exists")
    return dem_tmp_dir, links


def unlink(links: List) -> None:
    """
    Remove symlinks created
    :param links: List of links
    :return: None
    """
    for symlink in links:
        try:
            symlink.unlink()
            logger.info(" -- [Ok] Unlinked %s", symlink)
        except FileNotFoundError:
            logger.info("Cannot unlink %s", symlink)


def edit_xml_config_file(dem_type):
    """
    Edit xml config file depending on DEM used
    :param dem_type: DEM type
    """
    s2c_docker_cfg_file = "/root/sen2cor/2.9/cfg/L2A_GIPP.xml"
    tree = ET.parse(s2c_docker_cfg_file)
    root = tree.getroot()
    for name in root.iter("DEM_Directory"):
        name.text = f"dem/{dem_type}"
    for name in root.iter("DEM_Reference"):
        if dem_type == "srtm":
            name.text = (
                "http://srtm.csi.cgiar.org/wp-content/uploads/files/srtm_5x5/TIFF/"
            )
        elif dem_type == "copdem":
            name.text = "NONE"
        else:
            raise AttributeError("Attribute dem_type must be srtm or copdem")
    tree.write(s2c_docker_cfg_file, encoding="utf-8", xml_declaration=True)
    logger.info("%s --> edited with DEM infos", s2c_docker_cfg_file)


def set_sen2cor_log(loglevel: str) -> None:
    """
    Edit xml config file depending on DEM used
    :param dem_type: DEM type
    """
    s2c_docker_cfg_file = "/root/sen2cor/2.9/cfg/L2A_GIPP.xml"
    tree = ET.parse(s2c_docker_cfg_file)
    root = tree.getroot()
    for name in root.iter("Log_Level"):
        name.text = loglevel
    tree.write(s2c_docker_cfg_file, encoding="utf-8", xml_declaration=True)
    logger.info(f"Edited sen2cor loglevel to {loglevel}")


def walk(path: Path) -> Generator:
    """
    Recursively traverse all files from a directory.
    :param path: Directory path
    """
    for cur_path in path.iterdir():
        if cur_path.is_dir():
            yield from walk(cur_path)
            continue
        yield cur_path.resolve()


def execute_cmd(cmd: str) -> None:
    """
    Execute the given cmd.
    :param cmd: The command and its parameters to execute
    """
    logger.debug("Launching command: %s", cmd)
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as err:
        logger.error(
            "Following error code %s \
            occurred while running command %s with following output:\
            %s / %s",
            err.returncode,
            err.cmd,
            err.stdout,
            err.stderr,
        )
        raise
