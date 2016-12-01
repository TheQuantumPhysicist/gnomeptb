
# Script to simulate K&K counter files with approx 1 ms sampling rate
# the data is constant, only the times are updated
#
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

#Set the filename and open the file
outpath = 'D:\\ClockData\\Cavities\\'
sample = '160324 000000.002  30089915.15010000020   8587663.48739999905         0.0000000\n'
num_columns = 3
with_flags = False
fout_name = format(dt.datetime.now(),'%y%m%d') + '_1_Frequ.txt'

fout = open(outpath+fout_name,'w')
i = 0
sr = 1000
start_time = dt.datetime.now()
write_now = start_time
f = 0.07  # frequency
while True:  # just one hour
    real_now = dt.datetime.now()
    t = (write_now - start_time).total_seconds()
    data = np.array(['{:.20f}'.format(np.float64(i+1)*np.sin(2*math.pi*f*t)) for i in range(num_columns)])
    data_str = str(data).replace("[", "").replace("]", "").replace("'", "").replace("\n", "") + "\n"

    if (write_now - real_now).total_seconds() > 0:
        time.sleep((write_now - real_now).total_seconds())
    if i % sr == 0:
        write_now += dt.timedelta(seconds=1 / sr)
        s = format(write_now, '%y%m%d*%H%M%S.%f')[0:17] + (" FFFFFFFF" if with_flags else "") + " " + data_str
    else:
        write_now += dt.timedelta(seconds=1 / sr)
        s = format(write_now, '%y%m%d %H%M%S.%f')[0:17] + (" FFFFFFFF" if with_flags else "") + " " + data_str
    fout.write(s)
    fout.flush()
    i += 1
