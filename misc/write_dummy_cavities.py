
# Script to simulate K&K counter files with approx 1 ms sampling rate
# the data is constant, only the times are updated
#
#   US, PTB 2016


import time, os
import glob
import datetime as dt

#Set the filename and open the file
outpath = 'D:\\ClockData\\Cavities\\'
sample = '160324 000000.002  30089915.15010000020   8587663.48739999905         0.0000000\n'


fout_name = format(dt.datetime.now(),'%y%m%d') + '_1_Frequ.txt'

fout=open(outpath+fout_name,'w')
i=0
i1=0
sr=1000
write_now = dt.datetime.now()
while i < sr*60*60*1:  # just one hour
    real_now = dt.datetime.now()
    if (write_now - real_now).total_seconds() > 0:
        time.sleep((write_now - real_now).total_seconds())
    if i % sr == 0:
        write_now += dt.timedelta(seconds=1 / sr)
        s = format(write_now, '%y%m%d*%H%M%S.%f')[0:17] + sample[17:]
    else:
        write_now += dt.timedelta(seconds=1 / sr)
        s = format(write_now, '%y%m%d %H%M%S.%f')[0:17] + sample[17:]
    fout.write(s)
    fout.flush()
    i += 1
