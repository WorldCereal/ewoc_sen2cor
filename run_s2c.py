import json

import click
from dataship.dag.utils import get_product_by_id, l2a_to_ard
from ewoc_db.fill.update_status import get_next_tile

from utils import *

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
    click.secho("Run sen2cor", fg="green", bold=True)
    set_logger(verbose)


@cli.command("s2c_plan", help="Sen2cor with a full plan as input")
@click.option("-p", "--plan", help="EWoC Plan in json format")
@click.option("-o", "--l2a_dir", default=None, help="Output directory")
@click.option("-cfg", "--config", default=None, help="EOdag config file")
@click.option("-pv", "--provider", default=None, help="Data provider")
def run_plan(plan, l2a_dir, provider, config):
    if l2a_dir is None:
        l2a_dir = "/work/SEN2TEST/OUT/"
    dem_tmp_dir = "/work/SEN2TEST/DEM/"
    with open(plan) as f:
        plan = json.load(f)
    for tile in plan:
        if not os.path.exists(dem_tmp_dir):
            os.makedirs(dem_tmp_dir)
        custom_s2c_dem(tile, tmp_dir=dem_tmp_dir)
        count = 0
        prods = plan[tile]["S2_PROC"]["INPUTS"]
        for prod in prods:
            out_dir_l1c = os.path.join(l2a_dir, "tmp_L1C", tile)
            out_dir_l2a = os.path.join(l2a_dir, "tmp_L2A", tile)
            if not os.path.exists(out_dir_l1c):
                os.makedirs(out_dir_l1c)
            if not os.path.exists(out_dir_l2a):
                os.makedirs(out_dir_l2a)
            try:
                robust_get_by_id(prod, out_dir_l1c)
                logger.info(f"Download done for {prod}\n")
                l1c_safe_folder = [
                    os.path.join(out_dir_l1c, fold)
                    for fold in os.listdir(out_dir_l1c)
                    if fold.endswith("SAFE")
                ][0]
                l1c_safe_folder = last_safe(l1c_safe_folder)
                l2a_safe_folder = run_s2c(l1c_safe_folder, out_dir_l2a)
                l2a_to_ard(l2a_safe_folder, l2a_dir)
                clean(out_dir_l2a)
                clean(out_dir_l1c)
                ewoc_s3_upload(l2a_dir)
                count = +1
            except:
                logger.info(f'Something went wrong with {prod["id"]}')
        clean(dem_tmp_dir)
        number_of_products = len(prods)
        logger.info("\n\nEnd of processing ")
        # Check if
        logger.info(f"Processed {str(count)} of {str(number_of_products)}")


@cli.command("s2c_id", help="Sen2cor for on product using EOdag ID")
@click.option("-p", "--pid", help="S2 L1C product ID")
@click.option("-o", "--l2a_dir", default=None, help="Output directory")
@click.option("-cfg", "--config", default=None, help="EOdag config file")
@click.option("-pv", "--provider", default="creodias", help="Data provider")
def run_id(pid, l2a_dir, provider, config):
    if l2a_dir is None:
        l2a_dir = "/work/SEN2TEST/OUT/"
    # Generate temporary folders
    dem_tmp_dir = "/work/SEN2TEST/DEM/"
    tile = pid.split("_")[5][1:]
    if not os.path.exists(dem_tmp_dir):
        os.makedirs(dem_tmp_dir)
    dem_syms = custom_s2c_dem(tile, tmp_dir=dem_tmp_dir)
    out_dir_l1c, out_dir_l2a = make_tmp_dirs(l2a_dir)
    # Get Sat product by id using eodag
    robust_get_by_id(pid, out_dir_l1c)
    l1c_safe_folder = [
        os.path.join(out_dir_l1c, fold) for fold in os.listdir(out_dir_l1c) if fold.endswith("SAFE")
    ][0]
    l1c_safe_folder = last_safe(l1c_safe_folder)
    # Run sen2cor in subprocess
    l2a_safe_folder = run_s2c(l1c_safe_folder, out_dir_l2a)
    # Convert the sen2cor output to ewoc ard format
    l2a_to_ard(l2a_safe_folder, l2a_dir)
    # Delete local folders
    clean(out_dir_l2a)
    clean(out_dir_l1c)
    clean(dem_tmp_dir)
    unlink(dem_syms)
    # Send to s3
    ewoc_s3_upload(l2a_dir)


@cli.command("s2c_db", help="Sen2cor Postgreqsl mode")
@click.option("-e", "--executor", help="Name of the executor")
@click.option(
    "-f",
    "--status_filter",
    default="scheduled",
    help="Selects tiles that follow that condition",
)
def run_db(executor, status_filter):
    l2a_dir = "/work/SEN2TEST/OUT/"
    # Generate temporary folders
    dem_tmp_dir = "/work/SEN2TEST/DEM/"
    if not os.path.exists(dem_tmp_dir):
        os.makedirs(dem_tmp_dir)
    out_dir_l1c, out_dir_l2a = make_tmp_dirs(l2a_dir)
    # Get Sat product by id using eodag
    db_type = "fsmac"
    tile, _ = get_next_tile(db_type, executor, status_filter)
    pid = tile.products
    s2tile = pid.split("_")[5][1:]
    custom_s2c_dem(s2tile, tmp_dir=dem_tmp_dir)
    robust_get_by_id(pid, out_dir_l1c)
    logger.info(f"Download done for {pid}\n")
    # Make sure to get the right path to the SAFE folder!
    # TODO make this list comprehension more robust using regex
    l1c_safe_folder = [
        os.path.join(out_dir_l1c, fold) for fold in os.listdir(out_dir_l1c) if fold.endswith("SAFE")
    ][0]
    l1c_safe_folder = last_safe(l1c_safe_folder)
    ## Processing time, here sen2cor, could be another processor
    l2a_safe_folder = run_s2c(l1c_safe_folder, out_dir_l2a)
    # Convert the sen2cor output to ewoc ard format
    l2a_to_ard(l2a_safe_folder, l2a_dir)
    # Delete local folders
    clean(out_dir_l2a)
    clean(out_dir_l1c)
    # Send to s3
    ewoc_s3_upload(l2a_dir)
    ###### Update status of id on success
    tile.update_status(tile.id, db_type)


if __name__ == "__main__":
    cli()
