#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 17 20:07:49 2024

@author: atyagi
"""

import os
import gzip
import shutil
import fnmatch
import argparse
import subprocess
import numpy as np
import xarray as xr
from . import s3, cdo, datdir
from datetime import timedelta
from scipy.ndimage import convolve

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backup', action='store_true')
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--files', type=int, required=True)
    parser.add_argument('--grids', type=int, required=True)
    return parser.parse_args()

        
def create_dir(folder_name):
    try: shutil.rmtree(os.path.join('..', datdir, folder_name))
    except: pass
    current_directory = os.getcwd()
    parent_directory = os.path.abspath(os.path.join(current_directory, '..'))
    main_folder_path = os.path.join(parent_directory, datdir, folder_name)
    os.makedirs(main_folder_path, exist_ok=True)
    backup_folder_path = os.path.join(main_folder_path, 'backup')
    os.makedirs(backup_folder_path, exist_ok=True)
    subfolders = ['goes', 'hrrr', 'rf-10', 'elev']
    for subfolder in subfolders:
        subfolder_path = os.path.join(backup_folder_path, subfolder)
        os.makedirs(subfolder_path, exist_ok=True)


def list_files_s3(bucket, prefix):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        files = [obj['Key'] for obj in response['Contents']]
        return files
    else: return []


def elev_time(dirname, etime):
    cdo.remapnn("./mygrid", input="-setmisstoc,0 ./perm_elev.nc", options="-f nc4", output=f"../{datdir}/{dirname}/backup/elev/og_elev.nc")
    etime -= timedelta(hours=2)
    for i in range(2):
        etime += timedelta(hours=1)
        etime_str = etime.strftime("%Y-%m-%d,%H:%M:00,5min")
        cdo.settaxis(f"{etime_str}", input=f"../{datdir}/{dirname}/backup/elev/og_elev.nc", options="-f nc4 -r", output=f"../{datdir}/{dirname}/backup/elev/elev{i}.nc")
    etime -= timedelta(hours=1)
    etime_str = etime.strftime("%Y-%m-%d,%H:%M:00,5min")
    cdo.inttime(f"{etime_str}", input=f"-mergetime ../{datdir}/{dirname}/backup/elev/elev0.nc ../{datdir}/{dirname}/backup/elev/elev1.nc", options="-b F32 -f nc4 -r", output=f"../{datdir}/{dirname}/backup/elev.nc")


def merge_ins(dirname, ygrd, xgrd):
    try:
        ds1 = xr.open_dataset(f"../{datdir}/{dirname}/backup/goes.nc", chunks={'time': 1, 'lat': ygrd, 'lon': xgrd})
        ds2 = xr.open_dataset(f"../{datdir}/{dirname}/backup/hrrr.nc", chunks={'time': 1, 'lat': ygrd, 'lon': xgrd})
        ds3 = xr.open_dataset(f"../{datdir}/{dirname}/backup/elev.nc", chunks={'time': 1, 'lat': ygrd, 'lon': xgrd})
        ds = xr.merge([ds1, ds2, ds3])
        ds.to_zarr(f"../{datdir}/{dirname}/inputs.zarr", mode='w', consolidated=True)
    except: pass


def locate_data(indate, mrmsprod1, delaytime):
    flag = 1
    dname = indate.strftime("%Y%m%d_%H%M")
    indate -= timedelta(minutes=delaytime[1])
    pdate = indate - timedelta(hours=1)
    fdate = indate + timedelta(hours=1)
    pymd = pdate.strftime("%Y%m%d")
    fymd = fdate.strftime("%Y%m%d")
    
    if pymd == fymd:
        counter = 0
        doy = str(pdate.timetuple().tm_yday).zfill(3)
        gyr = pdate.strftime("%Y")
        prd1 = list_files_s3("noaa-mrms-pds", f"CONUS/{mrmsprod1}/{pymd}/")
        goes = list_files_s3("noaa-goes16", f"ABI-L1b-RadC/{gyr}/{doy}/00/")
        hrrr = list_files_s3("noaa-hrrr-bdp-pds", f"hrrr.{pymd}/conus/")
        if not prd1: counter+=1
        if not goes: counter+=1
        if not hrrr: counter+=1
        if counter > 0:
            with open("../data_info/warnings.txt", "a") as file: file.write(f"{dname} doesn't exist on AWS ({counter} pieces missing)" + "\n")
            flag = 0
    
    else:
        counter = 0
        doy = str(pdate.timetuple().tm_yday).zfill(3)
        gyr = pdate.strftime("%Y")
        pprd1 = list_files_s3("noaa-mrms-pds", f"CONUS/{mrmsprod1}/{pymd}/")
        pgoes = list_files_s3("noaa-goes16", f"ABI-L1b-RadC/{gyr}/{doy}/23/")
        phrrr = list_files_s3("noaa-hrrr-bdp-pds", f"hrrr.{pymd}/conus/")
        doy = str(fdate.timetuple().tm_yday).zfill(3)
        gyr = fdate.strftime("%Y")
        fprd1 = list_files_s3("noaa-mrms-pds", f"CONUS/{mrmsprod1}/{fymd}/")
        fgoes = list_files_s3("noaa-goes16", f"ABI-L1b-RadC/{gyr}/{doy}/00/")
        fhrrr = list_files_s3("noaa-hrrr-bdp-pds", f"hrrr.{fymd}/conus/")
        if not pprd1: counter+=1
        if not pgoes: counter+=1
        if not phrrr: counter+=1
        if not fprd1: counter+=1
        if not fgoes: counter+=1
        if not fhrrr: counter+=1
        if counter > 0:
            relcount = counter/2
            with open("../data_info/warnings.txt", "a") as file: file.write(f"{dname} doesn't exist on AWS ({relcount} pieces missing)" + "\n")
            flag = 0
            
    return flag


def process_data(dirname, remove):
    try:
        i = 0
        for prod in ["inputs", "mrms"]:
            ds = xr.open_zarr(f"../{datdir}/{dirname}/{prod}.zarr")
            for variable in ds.variables:
                for timestep in ds.time:
                    try:
                        data = ds[variable].sel(time=timestep).values
                        nan_indices = np.isnan(data)
                        if np.any(nan_indices):
                            i += 1
                    except: pass
        if i > 0:
            with open("../data_info/warnings.txt", "a") as file: file.write(f"{dirname} contains NaN" + "\n")
        inputs_num = 0
        target_num = 0
        folder_path = f"../{datdir}/{dirname}/inputs.zarr"
        for root, dirs, files in os.walk(folder_path): inputs_num += len(files)
        folder_path = f"../{datdir}/{dirname}/mrms.zarr"
        for root, dirs, files in os.walk(folder_path): target_num += len(files)
        if (inputs_num != 177 or target_num != 27):
            i+=1
            with open("../data_info/warnings.txt", "a") as file: file.write(f"{dirname} contains {inputs_num} inputs and {target_num} mrms" + "\n")
        print("\n" + f"Done processing {dirname}" + "\n")
    except:
        with open("../data_info/warnings.txt", "a") as file: file.write(f"{dirname} contains no zarr" + "\n")
        print("\n" + f"Done processing {dirname}" + "\n")


def check_inst(dirname, crtim, ref, num):
    flag = 0
    try:
        try: shutil.rmtree(f"../{datdir}/{dirname}/")
        except: pass
        os.makedirs(f"../{datdir}/{dirname}")
        crtim += timedelta(hours=1)
        if crtim.minute >= 30: crtim += timedelta(hours=1)
        crtim = crtim.replace(minute=0)
        date_str = crtim.strftime("%Y%m%d")
        time_str = crtim.strftime("%H%M")
        files_in_directory = list_files_s3("noaa-mrms-pds", f"CONUS/CREF_1HR_MAX_00.50/{date_str}/")
        matching_files = [file for file in files_in_directory if fnmatch.fnmatch(file, f"*{date_str}-{time_str}*")]
        file_down = matching_files[0]
        file_pt1 = crtim.strftime("%Y%m%d-%H%M")
        file_newname = f"CREF_1HR_MAX_00.50_{file_pt1}.grib2.gz"
        gbname = f"CREF_1HR_MAX_00.50_{file_pt1}.grib2"
        ncname = f"CREF_1HR_MAX_00.50_{file_pt1}.nc"
        rname = f"CREF_1HR_MAX_00.50_{file_pt1}_r.nc"
        s3.download_file("noaa-mrms-pds", file_down, f"../{datdir}/{dirname}/{file_newname}")
        with gzip.open(f"../{datdir}/{dirname}/{file_newname}", 'rb') as f_in, open(f"../{datdir}/{dirname}/{file_newname}"[:-3], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(f"../{datdir}/{dirname}/{file_newname}")
        tonc = [
            "bash", "-c",
            f"wgrib2 ../{datdir}/{dirname}/{gbname} -nc4 -netcdf ../{datdir}/{dirname}/{ncname}"
        ]
        subprocess.run(tonc)
        cdo.remapnn("./mygrid", input=f"../{datdir}/{dirname}/{ncname}", output=f"../{datdir}/{dirname}/{rname}")
        ds = xr.open_dataset(f"../{datdir}/{dirname}/{rname}")
        cref = ds["ReflectivityCompositeHourlyMax_500mabovemeansealevel"]
        thrs = xr.where(cref >= ref, 1, 0)
        if thrs.sum() >= num: flag = 1
        shutil.rmtree(f"../{datdir}/{dirname}/")
        ds.close()
    except: pass
    return flag


def make_target(dirname, ref, cape, cin, tch):
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
        newds["target"] = xr.where((tpar_sum >= 1) & (convolved >= (tch+1)), 1, 0)
        instances = newds["target"].sum().compute()
        print("\n" + f"Instances of convective initiation in {dirname}: {instances.values}" + "\n")
        with open("../data_info/instances.txt", "a") as file:
            file.write(f"Instances of convective initiation in {dirname}: {instances.values}" + "\n")
        newds.to_zarr(f"../{datdir}/{dirname}/target.zarr", mode='w', consolidated=True)
    except Exception as e:
        with open("../data_info/instances.txt", "a") as file:
            file.write(f"Error in {dirname}: {e}" + "\n")


__all__ = ['parse_args', 'create_dir', 'list_files_s3', 'elev_time', 'merge_ins', 'locate_data', 'process_data', 'check_inst', 'make_target']