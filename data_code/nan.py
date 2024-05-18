#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  3 22:50:00 2024

@author: atyagi
"""

import argparse
import numpy as np
import xarray as xr
from utils import datdir

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', type=str)
    parser.add_argument('--prod', type=str)
    return parser.parse_args()

def check_nan_values(dirname, prods):
    for prod in prods:
        print("")
        print(f"Checking {prod}...")
        print("")
        ds = xr.open_zarr(f"../{datdir}/{dirname}/{prod}.zarr")
        for variable in ds.variables:
            if variable not in ['lon', 'lat', 'time']:
                for timestep in ds.time:
                    try:
                        data = ds[variable].sel(time=timestep).values
                        nan_indices = np.isnan(data)
                        if np.any(nan_indices):
                            print(f"Variable: {variable}, Timestep: {timestep}")
                            print("NaN values found at indices:", np.where(nan_indices))
                            print("")
                    except Exception as e:
                        print("")
                        print(f"{variable} contains ERROR: {e}")
                        print("")

args = parse_args()
if args.prod: products = [args.prod]
else: products = ["inputs", "mrms"]
check_nan_values(args.dir, products)
print("done")