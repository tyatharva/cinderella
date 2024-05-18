#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import random
from utils import datdir

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', type=str)
    parser.add_argument('--print', action='store_true')
    parser.add_argument('--save', action='store_true')
    parser.add_argument('--plot', action='store_true')
    parser.add_argument('--prod', type=str, nargs='?')
    parser.add_argument('--var', type=str, nargs='?')
    parser.add_argument('--time', type=int, nargs='?')
    parser.add_argument('--geo', action='store_true')
    return parser.parse_args()

def randvar(ds):
    variables = [var for var in ds.variables if var not in ['time', 'lat', 'lon']]
    return random.choice(variables)

def main():
    args = parse_args()
    
    if args.prod: products = [args.prod]
    else: products = ['inputs', 'target']
    
    for prod in products:
        bigds = xr.open_zarr(f"../{datdir}/{args.dir}/{prod}.zarr")
        if args.var:
            if args.var in bigds: var_name = args.var
            else: var_name = randvar(bigds)
        else: var_name = randvar(bigds)
        if args.time:
            if args.time < bigds.dims['time']: timestamp = args.time
            else: timestamp = 0
        else: timestamp = 0
        vards = bigds[var_name]
        if prod == 'target': tslice = vards
        else: tslice = vards.isel(time=timestamp)
        
        if args.geo: ax = plt.axes(projection=ccrs.PlateCarree())
        if args.geo: ax.coastlines()
        if args.geo: ax.add_feature(cfeature.BORDERS, linestyle='--')
        if args.geo: ax.add_feature(cfeature.STATES, linestyle=':')
        plt.imshow(tslice, extent=(tslice.lon.min(), tslice.lon.max(), tslice.lat.min(), tslice.lat.max()), origin='lower')
        plt.colorbar()
        plt.title(f"{var_name} @ {timestamp}")
        
        if args.print: print(f"\n{tslice}\n")
        if args.save: plt.savefig(f"../{args.dir}_{var_name}_{timestamp}.png")
        if args.plot: plt.show()

if __name__ == "__main__":
    main()