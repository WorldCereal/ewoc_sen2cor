""" EWoC Sen2Cor processor CLI"""
import json
import logging
import os
from functools import reduce
from operator import concat
from pathlib import Path

import click
from ewoc_dag.bucket.creodias import CreodiasBucket
from ewoc_dag.s2_dag import get_s2_product
from ewoc_db.fill.update_status import get_next_tile

from ewoc_s2c.utils import (
    clean,
    custom_s2c_dem,
    edit_xml_config_file,
    ewoc_s3_upload,
    l2a_to_ard,
    last_safe,
    make_tmp_dirs,
    run_s2c,
    set_logger,
    unlink,
)

logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "--verbose",
    type=click.Choice(["v", "vv"]),
    default="vv",
    help="Set verbosity level: v for info, vv for debug",
    required=True,
)
def cli(verbose):
    """
    CLI
    :param verbose: verbose level
    :return: None
    """
    click.secho("Run sen2cor", fg="green", bold=True)
    set_logger(verbose)


@cli.command("s2c_plan", help="Sen2cor with a full plan as input")
@click.option("-p", "--plan", help="EWoC Workplan")
@click.option(
    "--production_id",
    default="0000",
    help="Production ID that will be used to upload to s3 bucket. " "Default: 0000",
)
@click.option("-ds", "--data_source", default="creodias")
@click.option(
    "-dem", "--dem_type", default="srtm", help="DEM that will be used in the process"
)
@click.option("-sc", "--only_scl", default=False, is_flag=True)
@click.option("--no_sen2cor", help="Do not process with Sen2cor", is_flag=True)
def run_plan(
    plan: str,
    production_id: str,
    data_source: str,
    dem_type: str,
    only_scl: bool,
    no_sen2cor: bool,
) -> None:
    """
    Run Sen2Cor with a json plan
    :param plan: WorkPlan in json format
    :param production_id: Special identifier
    :param data_source: Sentinel-2 data source
    :param only_scl: True to process scl only, default to False
    :param no_sen2cor: If True, download directly, no local atmospheric correction
    :return: None
    """

    l2a_dir = Path("/work/SEN2TEST/OUT/")
    l2a_dir.mkdir(exist_ok=True, parents=True)
    with open(plan, encoding="utf-8") as file_plan:
        plan_new = json.load(file_plan)
    tiles = plan_new["tiles"]
    for tile in tiles:
        dem_tmp_dir, dem_syms = custom_s2c_dem(dem_type, tile["tile_id"])
        count = 0
        prods = tile["s2_ids"]
        # Flatten list of s2 products
        prods = reduce(concat, prods)
        for pid in prods:
            upload_dir = l2a_dir / f"upload_{pid}"
            upload_dir.mkdir(exist_ok=True, parents=True)
            out_dir_l2a = l2a_dir / "tmp_L2A"
            out_dir_l2a.mkdir(exist_ok=True, parents=True)
            if not pid.endswith(".SAFE"):
                pid += ".SAFE"
            try:
                if "L2A" in pid and not no_sen2cor:
                    raise AttributeError("Using L2A product with Sen2cor is impossible")

                if no_sen2cor:
                    if only_scl:
                        scl_folder = get_s2_product(
                            pid, l2a_dir, source=data_source, l2_mask_only=True
                        )
                        l2a_to_ard(scl_folder, l2a_dir, only_scl)
                        ewoc_s3_upload(Path(upload_dir), production_id)
                    else:
                        raise NotImplementedError(
                            "Only the SCL MASK production is implemented"
                        )
                else:
                    # Get Sat product by id using ewoc_dag
                    if data_source == "aws":
                        l1c_safe_folder = get_s2_product(
                            pid, l2a_dir, source=data_source, aws_l1c_safe=True
                        )
                    else:
                        l1c_safe_folder = get_s2_product(
                            pid, l2a_dir, source=data_source
                        )
                    # Run sen2cor in subprocess
                    l2a_safe_folder = run_s2c(l1c_safe_folder, out_dir_l2a, only_scl)
                    # Convert the sen2cor output to ewoc ard format
                    l2a_to_ard(l2a_safe_folder, upload_dir, only_scl)
                    # Delete local folders
                    clean(out_dir_l2a)
                    # Send to s3
                    ewoc_s3_upload(Path(upload_dir), production_id)
                count = +1
            except RuntimeError:
                logger.info("Something went wrong with %s", pid)
        clean(dem_tmp_dir)
        unlink(dem_syms)
        number_of_products = len(prods)
        logger.info("\n\nEnd of processing ")
        logger.info("Processed %s of %s", str(count), str(number_of_products))


@cli.command("s2c_id", help="Sen2cor for on product using EOdag ID")
@click.option("-p", "--pid", help="S2 L1C product ID")
@click.option(
    "--production_id",
    default="0000",
    help="Production ID that will be used to upload to s3 bucket. " "Default: 0000",
)
@click.option("-ds", "--data_source", default="creodias")
@click.option(
    "-dem", "--dem_type", default="srtm", help="DEM that will be used in the process"
)
@click.option("-sc", "--only_scl", default=False, is_flag=True)
@click.option("--no_sen2cor", help="Do not process with Sen2cor", is_flag=True)
def run_id(
    pid: str,
    production_id: str,
    data_source: str,
    dem_type: str,
    only_scl: bool = False,
    no_sen2cor: bool = False,
) -> None:
    """
    Run Sen2Cor with a product ID
    :param pid: Sentinel-2 product identifier
    :param l2a_dir: Output folder
    :param production_id: Special identifier
    :param data_source: Sentinel-2 product data source
    :param dem_type: DEM type
    :param only_scl: True to process scl only
    :param no_sen2cor: Download directly, no local atmospheric correction
    :return: None
    """

    l2a_dir = Path("/work/SEN2TEST/OUT/")
    if os.path.exists(l2a_dir):
        clean(l2a_dir)
        logger.info("Cleared %s", l2a_dir)
    l2a_dir.mkdir(exist_ok=False, parents=True)
    upload_dir = l2a_dir / "upload"
    upload_dir.mkdir(exist_ok=True, parents=True)
    if not pid.endswith(".SAFE"):
        pid += ".SAFE"
    if "L2A" in pid and not no_sen2cor:
        raise AttributeError("Using L2A product with Sen2cor is impossible")

    if no_sen2cor:
        if only_scl:
            scl_folder = get_s2_product(
                pid, l2a_dir, source=data_source, l2_mask_only=True
            )
            l2a_to_ard(scl_folder, l2a_dir, only_scl)
            ewoc_s3_upload(upload_dir, production_id)
        else:
            raise NotImplementedError("Only the SCL MASK production is implemented")
    else:
        # Edit config file
        edit_xml_config_file(dem_type)
        # Download and create a DEM mosaic
        tile = pid.split("_")[5][1:]
        dem_tmp_dir, dem_syms = custom_s2c_dem(dem_type, tile)
        out_dir_l1c, out_dir_l2a = make_tmp_dirs(l2a_dir)
        # Get Sat product by id using ewoc_dag
        if data_source == "aws":
            l1c_safe_folder = get_s2_product(
                pid, out_dir_l1c, source=data_source, aws_l1c_safe=True
            )
        else:
            l1c_safe_folder = get_s2_product(pid, out_dir_l1c, source=data_source)
        # Run sen2cor in subprocess
        l2a_safe_folder = run_s2c(l1c_safe_folder, out_dir_l2a, only_scl)
        # Convert the sen2cor output to ewoc ard format
        l2a_to_ard(l2a_safe_folder, upload_dir, only_scl)
        # Delete local folders
        clean(out_dir_l2a)
        clean(dem_tmp_dir)
        unlink(dem_syms)
        # Send to s3
        ewoc_s3_upload(upload_dir, production_id)


@cli.command("s2c_db", help="Sen2cor Postgreqsl mode")
@click.option("-e", "--executor", help="Name of the executor")
@click.option(
    "-f",
    "--status_filter",
    default="scheduled",
    help="Selects tiles that follow that condition",
)
@click.option(
    "-dem", "--dem_type", default="srtm", help="DEM that will be used in the process"
)
@click.option(
    "--production_id",
    default="0000",
    help="Production ID that will be used to upload to s3 bucket. " "Default: 0000",
)
def run_db(executor, status_filter, dem_type, production_id) -> None:
    """
    Run Sen2Cor with a PostgreSQL database
    :param executor:
    :param status_filter:
    :param dem_type: DEM type
    :param production_id: Special identifier
    :return: None
    """
    l2a_dir = Path("/work/SEN2TEST/OUT/")
    # Generate temporary folders
    dem_tmp_dir = Path("/work/SEN2TEST/DEM/")
    dem_tmp_dir.mkdir(exist_ok=False, parents=True)
    out_dir_l1c, out_dir_l2a = make_tmp_dirs(l2a_dir)
    # Get Sat product by id using eodag
    db_type = "fsmac"
    tile, _ = get_next_tile(db_type, executor, status_filter)
    pid = tile.products
    s2tile = pid.split("_")[5][1:]
    custom_s2c_dem(dem_type, s2tile)
    bucket = CreodiasBucket()
    bucket.download_s2_prd(pid, Path(out_dir_l1c))
    logger.info("Download done for %s\n", pid)
    # Make sure to get the right path to the SAFE folder!
    # TODO make this list comprehension more robust using regex
    l1c_safe_folder = [
        out_dir_l1c / fold for fold in os.listdir(out_dir_l1c) if fold.endswith("SAFE")
    ][0]
    l1c_safe_folder = last_safe(l1c_safe_folder)
    # Processing time, here sen2cor, could be another processor
    l2a_safe_folder = run_s2c(l1c_safe_folder, out_dir_l2a)
    # Convert the sen2cor output to ewoc ard format
    l2a_to_ard(l2a_safe_folder, l2a_dir)
    # Delete local folders
    clean(out_dir_l2a)
    clean(out_dir_l1c)
    # Send to s3
    ewoc_s3_upload(l2a_dir, production_id)
    # Update status of id on success
    tile.update_status(tile.id, db_type)


if __name__ == "__main__":
    cli()
