#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import argparse
import multiprocessing
import numpy as np
import xarray as xr
from utils import datdir
from scipy.ndimage import convolve


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--remove', action='store_true', help='remove mrms.zarr (not reccomended)')
    parser.add_argument('--ref', type=int, required=True, help='Reflectivity change threshold (in dBz)')
    parser.add_argument('--cape', type=int, required=True, help='MUCAPE threshold (positive j/kg)')
    parser.add_argument('--cin', type=int, required=True, help='MUCIN threshold (negative j/kg)')
    parser.add_argument('--touch', type=int, required=True, help='Points touching valid point (includes diagonals)')
    parser.add_argument('--num', type=int, default=4, help='Number of concurrent processes')
    return parser.parse_args()


def process_directory(dirname, args):
    ref = args.ref
    cape = args.cape
    cin = args.cin
    tch = args.touch + 1
    try:
        mrms = xr.open_zarr(f"../{datdir}/{dirname}/mrms.zarr")
        ins = xr.open_zarr(f"../{datdir}/{dirname}/inputs.zarr")
        newds = xr.Dataset()
        tpar = []
        for i in range(0, 13):
            iref = mrms["ReflectivityM10C_500mabovemeansealevel"].isel(time=i)
            mucp = ins["CAPE_255M0mbaboveground"].isel(time=12)
            mucn = ins["CIN_255M0mbaboveground"].isel(time=12)
            rffg = xr.where(iref >= ref, 1, 0)
            cpfg = xr.where(mucp >= cape, 1, 0)
            cnfg = xr.where(mucn >= cin, 1, 0)
            targ = xr.where((rffg == 1) & (cpfg == 1) & (cnfg == 1), 1, 0)
            tpar.append(targ)
        tpar_concat = xr.concat(tpar, dim='time')
        tpar_sum = tpar_concat.sum(dim='time')
        time_coords = mrms["time"].isel(time=0)
        newds["time"] = time_coords
        kernel = np.ones((3, 3))
        target_data = tpar_sum.data
        convolved = convolve(target_data, kernel, mode='constant', cval=0)
        newds["target"] = xr.where((tpar_sum >= 1) & (convolved >= tch), 1, 0)
        instances = newds["target"].sum().compute()
        print("\n" + f"Instances of convective initiation in {dirname}: {instances.values}" + "\n")
        with open("../data_info/instances.txt", "a") as file:
            file.write(f"Instances of convective initiation in {dirname}: {instances.values}" + "\n")
        try:
            shutil.rmtree(f"../{datdir}/{dirname}/target.zarr/")
        except Exception as e:
            print(f"Error removing target.zarr: {e}")
        newds.to_zarr(f"../{datdir}/{dirname}/target.zarr", mode='w', consolidated=True)
    except Exception as e:
        with open("../data_info/instances.txt", "a") as file:
            file.write(f"Error in {dirname}: {e}" + "\n")


def main():
    try:
        os.remove("../data_info/instances.txt")
    except:
        pass
    args = parse_args()
    with open("../data_info/instances.txt", "a") as file:
        file.write(f"Rule: At least {args.ref} dBz reflectivity and {args.cape} j/kg of MUCAPE and at most {args.cin} j/kg of MUCIN and touching at least {args.touch} other point(s)\n")
    allitems = os.listdir(f"../{datdir}/")
    dirs = [item for item in allitems if os.path.isdir(os.path.join(f"../{datdir}/", item)) and item.startswith("20")]
    dirs = sorted(dirs)
    
    with multiprocessing.Pool(processes=args.num) as pool:
        pool.starmap(process_directory, [(dirname, args) for dirname in dirs])
    with open("../data_info/instances.txt", "r") as file:
        lines = file.readlines()
        lines = sorted(lines)
        lines.insert(0, lines.pop())
    with open("../data_info/instances.txt", "w") as file:
        file.writelines(lines)

if __name__ == "__main__":
    main()