import os
import json

import click

from dataship.dag.utils import l2a_to_ard, get_product_by_id



def run_s2c(l1c_safe,l2a_out):
    s2c_cmd = f"./Sen2Cor-02.09.00-Linux64/bin/L2A_Process {l1c_safe} --output_dir {l2a_out} --resolution 10"
    os.system(s2c_cmd)
    l2a_safe_folder = [os.path.join(l2a_out, fold) for fold in os.listdir(l2a_out) if fold.endswith('SAFE')][0]
    return l2a_safe_folder
def clean(folder):
    os.system(f"rm -r {folder}")
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

@click.group()
def cli():
    click.secho("Run sen2cor", fg="green",bold=True)

@cli.command('s2c',help="Sen2cor")
@click.option('-p', '--plan', help="EWoC Plan in json format")
@click.option('-o', '--l2a_dir', help="Output directory")
@click.option('-cfg', '--config', help="EOdag config file")
@click.option('-pv', '--provider', default = "creodias",help="Data provider")
def run_plan(plan, l2a_dir,provider,config):
    with open(plan) as f:
        plan = json.load(f)
    for tile in plan:
        out_dir = os.path.join(l2a_dir,"L1C",tile)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        prods = plan[tile]['S2_PROC']['INPUTS']
        for prod in prods[:3]:
            get_product_by_id(prod['id'], out_dir, provider, config_file=config)
            l1c_safe_folder = [os.path.join(out_dir, fold) for fold in os.listdir(out_dir) if fold.endswith('SAFE')][0]
            l1c_safe_folder = last_safe(l1c_safe_folder)
            print(f'Download done for {prod["id"]}\n')
            try:
                l2a_safe_folder = run_s2c(l1c_safe_folder,l2a_dir)
                l2a_to_ard(l2a_safe_folder,l2a_dir)
                clean(l2a_safe_folder)
                clean(l1c_safe_folder)
            except:
                print(f'Something went wrong with {prod["id"]}')
if __name__ == "__main__":
    cli()
