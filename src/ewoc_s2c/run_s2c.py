""" EWoC Sen2Cor processor CLI"""
import logging
import os
from pathlib import Path

import click
from ewoc_dag.eo_prd_id.s2_prd_id import S2PrdIdInfo
from ewoc_dag.s2_dag import get_s2_product

from ewoc_s2c.utils import (
    clean,
    custom_s2c_dem,
    edit_xml_config_file,
    ewoc_s3_upload,
    l2a_to_ard,
    l2a_to_ard_aws_cog,
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
    help="Set verbosity level: v for info, vv for debug",
    required=False,
)
def cli(verbose):
    """
    CLI
    :param verbose: verbose level
    :return: None
    """
    click.secho("Run sen2cor", fg="green", bold=True)
    set_logger(verbose)


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
def run_id(
    pid: str,
    production_id: str,
    data_source: str,
    dem_type: str,
    only_scl: bool = False,
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
    if S2PrdIdInfo.is_l2a(pid):
        if data_source == "aws":
            # Only aws cog option supported in full
            try:
                l2a_folder = get_s2_product(
                    pid,
                    l2a_dir,
                    source=data_source,
                    l2_mask_only=only_scl,
                    aws_l2a_cogs=True)
            except:
                try:
                    date_acq=pid.split('_')[2][:8]
                    date_end=pid.split('_')[-1][:8]
                    if date_acq!=date_end:
                        pid=pid.replace(date_acq, date_end)
                    l2a_folder = get_s2_product(
                    pid,
                    l2a_dir,
                    source=data_source,
                    l2_mask_only=only_scl,
                    aws_l2a_cogs=True)
                except:
                    logger.error("The product"+pid+"does not exist")
            l2a_to_ard_aws_cog(l2a_folder, upload_dir, data_source, only_scl)
            ewoc_s3_upload(upload_dir, production_id)
        elif data_source == "aws_sng":
            try:
                l2a_folder = get_s2_product(
                    pid,
                    l2a_dir,
                    source="aws_sng",
                    l2_mask_only=only_scl,
                    aws_l2a_cogs=False)
            except:
                try:
                    date_acq=pid.split('_')[2][:8]
                    date_end=pid.split('_')[-1][:8]
                    if date_acq!=date_end:
                        pid=pid.replace(date_acq, date_end)
                    l2a_folder = get_s2_product(
                    pid,
                    l2a_dir,
                    source="aws_sng",
                    l2_mask_only=only_scl,
                    aws_l2a_cogs=False)
                except:
                    logger.error("The product"+pid+"does not exist")
            logger.info("Product downloaded from Sinergise bucket")
            l2a_to_ard(l2a_folder, upload_dir, pid, data_source, only_scl)
            ewoc_s3_upload(upload_dir, production_id)
        elif data_source == "creodias":
            try:
                l2a_folder = get_s2_product(
                    pid,
                    l2a_dir,
                    source=data_source,
                    l2_mask_only=only_scl)
            except:
                try:
                    date_acq=pid.split('_')[2][:8]
                    date_end=pid.split('_')[-1][:8]
                    if date_acq!=date_end:
                        pid=pid.replace(date_acq, date_end)
                    l2a_folder = get_s2_product(
                    pid,
                    l2a_dir,
                    source=data_source,
                    l2_mask_only=only_scl)
                except:
                    logger.error("The product"+pid+"does not exist")
            l2a_to_ard(l2a_folder, upload_dir, pid, data_source, only_scl)
            ewoc_s3_upload(upload_dir, production_id)
        else:
            logger.warning("%s is not supported (yet) for L2A ids", data_source)
    else:
        # Edit config file
        edit_xml_config_file(dem_type)
        # Download and create a DEM mosaic
        tile = pid.split("_")[5][1:]
        dem_tmp_dir, dem_syms = custom_s2c_dem(dem_type, tile)
        out_dir_l1c, out_dir_l2a = make_tmp_dirs(l2a_dir)
        # Get Sat product by id using ewoc_dag
        if data_source == "aws_sng":
            try:
                l1c_safe_folder = get_s2_product(
                    pid,
                    out_dir_l1c,
                    source='aws',
                    aws_l1c_safe=True,
                    aws_l2a_cogs=False)
            except:
                try:
                    date_acq=pid.split('_')[2][:8]
                    date_end=pid.split('_')[-1][:8]
                    if date_acq!=date_end:
                        pid=pid.replace(date_acq, date_end)
                    l1c_safe_folder = get_s2_product(
                    pid,
                    out_dir_l1c,
                    source='aws',
                    aws_l1c_safe=True,
                    aws_l2a_cogs=False)
                except:
                    logger.error("The product"+pid+"does not exist")
        else:
            try:
                l1c_safe_folder = get_s2_product(pid, out_dir_l1c, source=data_source)
            except:
                try:
                    date_acq=pid.split('_')[2][:8]
                    date_end=pid.split('_')[-1][:8]
                    if date_acq!=date_end:
                        pid=pid.replace(date_acq, date_end)
                    l1c_safe_folder = get_s2_product(pid, out_dir_l1c, source=data_source)
                except:
                    logger.error("The product"+pid+"does not exist")            
        # Run sen2cor in subprocess
        l2a_safe_folder = run_s2c(l1c_safe_folder, out_dir_l2a, only_scl)
        # Convert the sen2cor output to ewoc ard format
        l2a_to_ard(l2a_safe_folder, upload_dir, pid, data_source, only_scl)
        # Delete local folders
        clean(out_dir_l2a)
        clean(dem_tmp_dir)
        unlink(dem_syms)
        # Send to s3
        ewoc_s3_upload(upload_dir, production_id)


if __name__ == "__main__":
    cli()
