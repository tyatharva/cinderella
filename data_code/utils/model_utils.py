#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import gzip
import shutil
import fnmatch
import subprocess
import pandas as pd
import xarray as xr
from . import s3, cdo, datdir
from herbie import FastHerbie
from datetime import timedelta
from utils.helper_utils import list_files_s3


def mrms(dirname, product_long, product_short, mtime, delay, ygrd, xgrd):
    
    gettim2 = mtime - timedelta(minutes=delay[0])
    modtime = gettim2.minute % 2
    if modtime != 0: gettim2 -= timedelta(minutes=modtime)
    
    x = 0
    gettim2 += timedelta(minutes=2)
    while x < 31:
        date_str = gettim2.strftime("%Y%m%d")
        time_str = gettim2.strftime("%H%M")
        files_in_directory = list_files_s3("noaa-mrms-pds", f"CONUS/{product_long}/{date_str}/")
        matching_files = [file for file in files_in_directory if fnmatch.fnmatch(file, f"*{date_str}-{time_str}*")]

        if matching_files:
            file_down = matching_files[0]
            file_pt1 = gettim2.strftime("%Y%m%d-%H%M")
            file_newname = f"{product_short}_{file_pt1}.grib2.gz"
            s3.download_file("noaa-mrms-pds", file_down, f"../{datdir}/{dirname}/backup/{product_short}/{file_newname}")
            print(f"{file_newname} downloaded successfully.")
            x += 1
        gettim2 += timedelta(minutes=2)
    
    files_to_process = glob.glob(f"../{datdir}/{dirname}/backup/{product_short}/*.gz")
    files_to_process = sorted(files_to_process)

    for file in files_to_process:
        with gzip.open(file, 'rb') as f_in, open(file[:-3], 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(file)
    
    tonc = [
        "bash", "-c",
        f"for file in ../{datdir}/{dirname}/backup/{product_short}/*.grib2; do wgrib2 \"$file\" -nc4 -netcdf \"${{file%.grib2}}.nc\"; done"
    ]
    subprocess.run(tonc)
    
    mergetime = [
        "cdo",
        "-f", "nc4",
        "mergetime",
        f"../{datdir}/{dirname}/backup/{product_short}/*.nc",
        f"../{datdir}/{dirname}/backup/{product_short}/{product_short}tmp.nc"
    ]
    subprocess.run(mergetime)
    
    mtime += timedelta(minutes=5)
    itime = mtime.strftime("%Y-%m-%d,%H:%M:00")
    cdo.inttime(f"{itime},5min", input=f"-settaxis,{itime},2min -setmisstoc,0 -setrtomiss,-1000,0 ../{datdir}/{dirname}/backup/{product_short}/{product_short}tmp.nc", options='-f nc4 -r', output=f"../{datdir}/{dirname}/backup/{product_short}/{product_short}tmpp.nc")
    
    stime = mtime.strftime("%Y-%m-%d,%H:%M:00,5min")
    settaxis = [
        "cdo",
        "-f", "nc4", "-r",
        f"settaxis,{stime}",
        f"../{datdir}/{dirname}/backup/{product_short}/{product_short}tmpp.nc",
        f"../{datdir}/{dirname}/backup/{product_short}/{product_short}tmppp.nc"
    ]
    subprocess.run(settaxis)
    
    remap = [
        "cdo",
        "-b", "F32", "-f", "nc4",
        "remapnn,./mygrid",
        f"../{datdir}/{dirname}/backup/{product_short}/{product_short}tmppp.nc",
        f"../{datdir}/{dirname}/backup/{product_short}.nc"
    ]
    subprocess.run(remap)
    
    remove = [f"rm ../{datdir}/{dirname}/backup/{product_short}/*.nc"]
    subprocess.run(remove, shell=True)
    
    try:
        ds = xr.open_dataset(f"../{datdir}/{dirname}/backup/rf-10.nc", chunks={'time': 1, 'lat': ygrd, 'lon': xgrd})
        ds.to_zarr(f"../{datdir}/{dirname}/mrms.zarr", mode='w', consolidated=True)
    except: pass


def mfilerdir_hrrr(directory):
    items = os.listdir(directory)
    for item in items:
        item_path = os.path.join(directory, item)
        if os.path.isdir(item_path):
            for root, dirs, files in os.walk(item_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    new_file_name = file.split("__", 1)[-1]
                    original_folder_name = os.path.basename(item_path)
                    new_path = os.path.join(directory, original_folder_name + "_" + new_file_name)
                    shutil.move(file_path, new_path)
            shutil.rmtree(item_path)


def hrrr(dirname, htime, thds, delay):
    
    hrtime = htime - timedelta(hours=1, minutes=delay[1])
    hrtime = hrtime.replace(minute=0)
    DATES = pd.date_range(start=hrtime.strftime("%Y-%m-%d %H:00"), periods=2, freq="1H",)
    fxx=range(0,1)
    data = FastHerbie(DATES, model="hrrr", product="prs", fxx=fxx, max_threads=thds,)
    data.download(searchString="PWAT|(VVEL:(700|850|925)|(CAPE:255)|(CIN:255)|(HGT:equilibrium level)|(HGT:((reserved)|(no_level)|(level of free convection))))", max_threads=thds, save_dir = f"../{datdir}/{dirname}/backup/")
    mfilerdir_hrrr(f"../{datdir}/{dirname}/backup/hrrr/")
    
    tonc = [
        "bash", "-c",
        f"for file in ../{datdir}/{dirname}/backup/hrrr/*.grib2; do wgrib2 \"$file\" -nc4 -netcdf \"${{file%.grib2}}.nc\"; done"
    ]
    subprocess.run(tonc)
    
    tstime = htime - timedelta(hours=1)
    stime = tstime.strftime("%Y-%m-%d,%H:%M:00,5min")
    ltime = hrtime.strftime("%Y-%m-%d,%H:%M:00,5min")
    h1 = hrtime.strftime("%H")
    hrtime += timedelta(hours=1)
    h2 = hrtime.strftime("%H")
    f1 = glob.glob(f"../{datdir}/{dirname}/backup/hrrr/*t{h1}z*.nc")[0]
    f2 = glob.glob(f"../{datdir}/{dirname}/backup/hrrr/*t{h2}z*.nc")[0]
    cdo.remapnn("./mygrid", input=f"-delname,HGT_equilibriumlevel,HGT_leveloffreeconvection -aexpr,'convdepth=((HGT_equilibriumlevel-HGT_leveloffreeconvection)>=0)?(HGT_equilibriumlevel-HGT_leveloffreeconvection):0' -chname,HGT_no_level,HGT_leveloffreeconvection -chname,HGT_reserved,HGT_leveloffreeconvection -settaxis,{stime} -inttime,{ltime} -mergetime {f1} {f2}", options=f"-b F32 -P {thds} -f nc4 -r", output=f"../{datdir}/{dirname}/backup/hrrr.nc")
    
    remove = [f"rm ../{datdir}/{dirname}/backup/hrrr/*.nc"]
    subprocess.run(remove, shell=True)


def goes(dirname, gtime, delay):
    
    gettime = gtime - timedelta(minutes=delay[2])
    i = 0
    while i < 13:
        minute_str = gettime.strftime("%M").zfill(2)
        hour_str = gettime.strftime("%H").zfill(2)
        doy_str = str(gettime.timetuple().tm_yday).zfill(3)
        year_str = gettime.strftime("%Y").zfill(4)
        files_in_directory = list_files_s3("noaa-goes16", f"ABI-L2-MCMIPC/{year_str}/{doy_str}/{hour_str}/")
        matching_files = [file for file in files_in_directory if fnmatch.fnmatch(file, f"*_G16_s???????{hour_str}{minute_str}*.nc")]

        if matching_files:
            file_down = matching_files[0]
            file_newname = gettime.strftime("%Y%m%d-%H%M.nc")
            s3.download_file("noaa-goes16", file_down, f"../{datdir}/{dirname}/backup/goes/{file_newname}")
            print(f"GOES-16 {file_newname} downloaded successfully.")
            i += 1
        gettime -= timedelta(minutes=1)
    
    file_paths = glob.glob(f"../{datdir}/{dirname}/backup/goes/*.nc")
    for file_path in file_paths:
        fnext = os.path.basename(file_path)
        fnnext = os.path.splitext(fnext)[0]
        cdo.selname('CMI_C02,CMI_C07,CMI_C13', input=f"{file_path}", options='-f nc4', output=f"../{datdir}/{dirname}/backup/goes/{fnnext}_tmp1.nc")
        for band in ['CMI_C02', 'CMI_C07', 'CMI_C13']:
            gdal = [
                "bash", "-c",
                f"gdalwarp -q -s_srs \"+proj=geos +h=35786023.0 +a=6378137.0 +b=6356752.31414 +f=0.0033528106647475126 +lon_0=-75.0 +sweep=x +no_defs\" -t_srs EPSG:4326 -r near NETCDF:\"../{datdir}/{dirname}/backup/goes/{fnnext}_tmp1.nc\":{band} ../{datdir}/{dirname}/backup/goes/{fnnext}_{band}.nc"
            ]
            subprocess.run(gdal)
            cdo.chname(f"Band1,{band}", input=f"../{datdir}/{dirname}/backup/goes/{fnnext}_{band}.nc", output=f"../{datdir}/{dirname}/backup/goes/{fnnext}_{band}_r.nc")
        cdo.merge(input=f"../{datdir}/{dirname}/backup/goes/{fnnext}_CMI_C02_r.nc ../{datdir}/{dirname}/backup/goes/{fnnext}_CMI_C07_r.nc ../{datdir}/{dirname}/backup/goes/{fnnext}_CMI_C13_r.nc", output=f"../{datdir}/{dirname}/backup/goes/{fnnext}_tmp2.nc")
    
    files = glob.glob(f"../{datdir}/{dirname}/backup/goes/*_tmp2.nc")
    files = sorted(files)
    for file in files:
        gettime = gettime + timedelta(minutes=5)
        newname = gettime.strftime("%Y%m%d_%H%M_tmp3")
        cdo.settaxis(gettime.strftime("%Y-%m-%d,%H:%M:00,5min"), input=f"{file}", options='-f nc4 -r', output=f"../{datdir}/{dirname}/backup/goes/{newname}.nc")
    
    gtime -= timedelta(hours=1)
    time_str = gtime.strftime("%Y-%m-%d,%H:%M:00,5min")
    cdo.remapnn('./mygrid', input=f"-settaxis,{time_str} -setmisstoc,0 -mergetime ../{datdir}/{dirname}/backup/goes/*_tmp3.nc", options="-b F32 -f nc4 -r", output=f"../{datdir}/{dirname}/backup/goes.nc")
    
    remove = [f"rm ../{datdir}/{dirname}/backup/goes/*_tmp?.nc"]
    subprocess.run(remove, shell=True)


__all__ = ['mrms', 'mfilerdir_hrrr', 'hrrr', 'goes']