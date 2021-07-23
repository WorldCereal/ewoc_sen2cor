import os
import json
import shutil

import click

from utils import run_s2c, clean, last_safe
from dataship.dag.utils import l2a_to_ard, get_product_by_id

@click.group()
def cli():
    click.secho("Run sen2cor", fg="green",bold=True)

@cli.command('s2c_plan',help="Sen2cor with a full plan as input")
@click.option('-p', '--plan', help="EWoC Plan in json format")
@click.option('-o', '--l2a_dir', help="Output directory")
@click.option('-cfg', '--config', help="EOdag config file")
@click.option('-pv', '--provider', default = "creodias",help="Data provider")
def run_plan(plan, l2a_dir,provider,config):
    with open(plan) as f:
        plan = json.load(f)
    for tile in plan:
        prods = plan[tile]['S2_PROC']['INPUTS']
        for prod in prods:
            out_dir_l1c = os.path.join(l2a_dir, "tmp_L1C", tile)
            out_dir_l2a = os.path.join(l2a_dir, "tmp_L2A", tile)
            if not os.path.exists(out_dir_l1c):
                os.makedirs(out_dir_l1c)
            if not os.path.exists(out_dir_l2a):
                os.makedirs(out_dir_l2a)
            get_product_by_id(prod['id'], out_dir_l1c, provider, config_file=config)
            l1c_safe_folder = [os.path.join(out_dir_l1c, fold) for fold in os.listdir(out_dir_l1c) if fold.endswith('SAFE')][0]
            l1c_safe_folder = last_safe(l1c_safe_folder)

            print(f'Download done for {prod["id"]}\n')
            try:
                l2a_safe_folder = run_s2c(l1c_safe_folder,out_dir_l2a)
                l2a_to_ard(l2a_safe_folder,l2a_dir)
                clean(out_dir_l2a)
                clean(out_dir_l1c)
            except:
                print(f'Something went wrong with {prod["id"]}')
@cli.command('s2c_id',help="Sen2cor for on product using EOdag ID")
@click.option('-p', '--pid', help="S2 L1C product ID")
@click.option('-o', '--l2a_dir', help="Output directory")
@click.option('-cfg', '--config', help="EOdag config file")
@click.option('-pv', '--provider', default = "creodias",help="Data provider")
def run_id(pid, l2a_dir,provider,config):
        out_dir_l1c = os.path.join(l2a_dir, "tmp_L1C")
        out_dir_l2a = os.path.join(l2a_dir, "tmp_L2A")
        if not os.path.exists(out_dir_l1c):
            os.makedirs(out_dir_l1c)
        if not os.path.exists(out_dir_l2a):
            os.makedirs(out_dir_l2a)
        get_product_by_id(pid, out_dir_l1c, provider, config_file=config)
        l1c_safe_folder = [os.path.join(out_dir_l1c, fold) for fold in os.listdir(out_dir_l1c) if fold.endswith('SAFE')][0]
        l1c_safe_folder = last_safe(l1c_safe_folder)

        print(f'Download done for {pid}\n')
        try:
            l2a_safe_folder = run_s2c(l1c_safe_folder,out_dir_l2a)
            l2a_to_ard(l2a_safe_folder,l2a_dir)
            clean(out_dir_l2a)
            clean(out_dir_l1c)
        except:
            print(f'Something went wrong with {pid}')
if __name__ == "__main__":
    cli()
