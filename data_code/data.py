#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import random
import shutil
import multiprocessing
import numpy as np
from datetime import datetime, timedelta
from utils import datdir
from utils.model_utils import hrrr, mrms, goes
from utils.helper_utils import parse_args, create_dir, elev_time, merge_ins, locate_data, process_data, check_inst, make_target


def main():
    
    #initial things
    st = time.time()
    args = parse_args()
    tout = 500
    total_att = 6
    try: shutil.rmtree("../data_info")
    except: pass
    os.makedirs("../data_info", exist_ok=True)
    os.environ["REMAP_EXTRAPOLATE"] = "off"
    # mrms, hrrr, goes
    delaytimes = [3, 55, 5]
    ref = 35
    cape = 100
    cin = -50
    tch = 3
    gridtype = "lonlat"
    xsize = 250
    ysize = 250
    xinc = 0.02
    yinc = 0.02
    fpd = args.files
    gps = args.grids
    stdate_gb = datetime.strptime(args.start,"%Y%m%d")
    eddate_gb = datetime.strptime(args.end,"%Y%m%d")
    step_gb = timedelta(days=1)
    files_done = 0
    prev_dirname = "20000101_0000_1"
    
    # loop through days
    for i in range((eddate_gb - stdate_gb).days +1):
        # increment day
        date_cr = stdate_gb + i * step_gb
        # loop through time sections in day
        for s in range(fpd):
            fnames = set()
            fnames.clear()
            g = 0
            atts = 0
            # loop through selections per time section
            while g < gps:
                z = 0
                # choose a random geographical area
                xfirst = round(random.uniform(-116.1, -76.1), 2)
                yfirst = round(random.uniform(25, 45), 2)
                grid_specs = f"""gridtype = {gridtype}
                xsize    = {xsize}
                ysize    = {ysize}
                xfirst   = {xfirst}
                xinc     = {xinc}
                yfirst   = {yfirst}
                yinc     = {yinc}
                """
                with open("./mygrid", "w") as file: file.write(grid_specs)
                # get a time that hasn't already been taken
                while z == 0:
                    hour_cr = np.random.randint(s*(24/fpd), (s*(24/fpd))+(24/fpd))
                    minute_cr = np.random.randint(0, 12) * 5
                    datetime_cr = date_cr + timedelta(hours=hour_cr, minutes=minute_cr)
                    dirName = datetime_cr.strftime("%Y%m%d_%H%M")
                    if dirName not in fnames: z = 1
                # check if it exists
                if locate_data(datetime_cr, "Reflectivity_-10C_00.50", delaytimes) == 1:
                    # check if it has potential to have hits in the target
                    if check_inst(dirName, datetime_cr, 40, 40) == 1:
                        lst = time.time()
                        # add it to the set of times retrieved for this time section
                        fnames.add(dirName)
                        print("\n" + datetime_cr.strftime("%Y-%m-%d %H:%M") + " has been found\n")
                        check = multiprocessing.Process(target=process_data, args=(prev_dirname, True, ))
                        target = multiprocessing.Process(target=make_target, args=(prev_dirname, ref, cape, cin, tch, ))
                        if files_done > 0: check.start()
                        attempt = 1
                        while attempt <= total_att:
                            create_dir(dirName)
                            tfm_rf10 = multiprocessing.Process(target=mrms, args=(dirName, "Reflectivity_-10C_00.50", "rf-10", datetime_cr, delaytimes, ysize, xsize, ))
                            tfm_hrrr = multiprocessing.Process(target=hrrr, args=(dirName, datetime_cr, 1, delaytimes, ))
                            tfm_goes = multiprocessing.Process(target=goes, args=(dirName, datetime_cr, delaytimes, ))
                            tfm_elev = multiprocessing.Process(target=elev_time, args=(dirName, datetime_cr, ))
                            mrge_file = multiprocessing.Process(target=merge_ins, args=(dirName, ysize, xsize, ))
                            tfm_goes.start()
                            tfm_rf10.start()
                            tfm_hrrr.start()
                            tfm_elev.start()
                            if files_done > 0:
                                check.join(tout)
                                target.start()
                            tfm_elev.join(tout)
                            tfm_hrrr.join(tout)
                            tfm_goes.join(tout)
                            mrge_file.start()
                            mrge_file.join(tout)
                            if files_done > 0: target.join(tout)
                            tfm_rf10.join(tout)
                            if not (os.path.exists(f"../{datdir}/{dirName}/inputs.zarr/") and os.path.exists(f"../{datdir}/{dirName}/mrms.zarr/")):
                                if attempt != total_att:
                                    shutil.rmtree(f"../{datdir}/{dirName}/")
                                    err = f"{dirName} retry #{attempt} (attempt #{attempt+1})"
                                    print("\n\n" + err + "\n\n")
                                    with open("../data_info/retries.txt", "a") as file: file.write(err + "\n")
                                attempt += 1
                                time.sleep(1)
                            else: attempt = total_att + 1
                        if not args.backup: shutil.rmtree(f"../{datdir}/{dirName}/backup/")
                        let = time.time()
                        lti = round(let-lst, 3)
                        timing = f"{dirName} done in {lti} seconds\n"
                        with open("../data_info/timings.txt", "a") as file: file.write(timing)
                        print("\n" + timing)
                        shutil.copy("./mygrid", f"../{datdir}/{dirName}/grid.txt")
                        prev_dirname = dirName
                        atts = 0
                        files_done += 1
                        g+=1
                    
                    else:
                        atts+=1
                        if atts >= total_att*6:
                            print("\nGrid not found for " + datetime_cr.strftime("%Y-%m-%d %H:%M") + "\n")
                            with open("../data_info/warnings.txt", "a") as file: file.write("\nGrid not found for " + datetime_cr.strftime("%Y-%m-%d %H:%M") + "\n")
                            atts = 0
                            g+=1
                else:
                    print("\n" + datetime_cr.strftime("%Y-%m-%d %H:%M") + " does not exist\n")
                    g+=1

    if files_done > 0:
        process_data(prev_dirname, True)
        make_target(prev_dirname, ref, cape, cin, tch)
        with open("../data_info/instances.txt", "r") as file:
            lines = file.readlines()
            lines = sorted(lines)
            lines.insert(0, f"Rule: At least {ref} dBz reflectivity and {cape} j/kg of MUCAPE and at most {cin} j/kg of MUCIN and touching at least {tch} other point(s)\n")
        with open("../data_info/instances.txt", "w") as file:
            file.writelines(lines)
    et = time.time()
    ti = et - st
    print("\n" + f"Completed {files_done} files from {args.start} to {args.end} with {args.files} files per day and in {args.grids} grids per step in ", ti, " seconds\n")


if __name__ == "__main__":
    main()