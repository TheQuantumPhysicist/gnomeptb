
# Script to simulate K&K counter files from frequency comb with approx 1 s sampling rate
# the data is constant, only the times are updated
#
#   US, PTB 2016
#   US, PTB 2016

##################
# Script modified by Samer Afach (Uni-Mainz), 01.12.2016
# The script now produces time coherent data that simulates atomic clocks experiment output
# The output is simply a sinosoid with frequency f
##################

import time, os
import glob
import datetime as dt
import numpy as np
import math
import copy

#Set the filename and open the file
outpath = 'D:\\ClockData\\comb\\'
sample = '161101*000000.299 FFFFFFFF  32365919.99990969900  19999999.98439349980  23359875.19833400100  23572738.83385109900  51320112.94533299650  54701611.62395049630  62468051.41144129630  34201269.49379400160  34999999.99953600020  20000000.00156589970  10000000.00132700060  59725627.47657729680  19999999.98443600160  23359875.19851360100  23572738.83431870120  51320112.96735619750  61425204.12941499800  62468049.96998640150  46581938.69217439740  46581938.69214440140         1.4414549\n'
num_columns = 21
with_flags = True
def fout_name(currdate):
    return os.path.join(outpath,format(currdate,'%y%m%d') + '_1_Frequ.txt')

fout = open(fout_name(dt.datetime.now()),'w')
i = 0
sr = 1
start_time = dt.datetime.now()
write_now = start_time
prev_write_now = start_time
f = 0.07  # frequency of the signal
while True:
    real_now = dt.datetime.now()
    t = (write_now - start_time).total_seconds()
    data = np.array(['{:.20f}'.format(np.float64(i+1)*np.sin(2*math.pi*f*t)) for i in range(num_columns)])
    data_str = str(data).replace("[", "").replace("]", "").replace("'", "").replace("\n", "") + "\n"

    # switch file for next day
    if write_now.date() != prev_write_now.date():
        fout.close()
        fout = open(fout_name(write_now), 'w')

    if (write_now - real_now).total_seconds() > 0:
        time.sleep((write_now - real_now).total_seconds())

    prev_write_now = copy.deepcopy(write_now)
    if i % sr == 0:
        write_now += dt.timedelta(seconds=1 / sr)
        s = format(write_now, '%y%m%d*%H%M%S.%f')[0:17] + (" FFFFFFFF" if with_flags else "") + " " + data_str
    else:
        write_now += dt.timedelta(seconds=1 / sr)
        s = format(write_now, '%y%m%d %H%M%S.%f')[0:17] + (" FFFFFFFF" if with_flags else "") + " " + data_str
    fout.write(s)
    fout.flush()
    i += 1
