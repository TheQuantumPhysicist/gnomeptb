
# Script to simulate K&K counter files from frequency comb with approx 1 s sampling rate
# the data is constant, only the times are updated
#
#   US, PTB 2016
#   US, PTB 2016


import time, os
import glob
import datetime as dt

#Set the filename and open the file
outpath = 'D:\\ClockData\\comb\\'
sample = '161101*000000.299 FFFFFFFF  32365919.99990969900  19999999.98439349980  23359875.19833400100  23572738.83385109900  51320112.94533299650  54701611.62395049630  62468051.41144129630  34201269.49379400160  34999999.99953600020  20000000.00156589970  10000000.00132700060  59725627.47657729680  19999999.98443600160  23359875.19851360100  23572738.83431870120  51320112.96735619750  61425204.12941499800  62468049.96998640150  46581938.69217439740  46581938.69214440140         1.4414549\n'

fout_name = format(dt.datetime.now(),'%y%m%d') + '_1_Frequ.txt'

fout=open(outpath+fout_name,'w')
i=0
i1=0
sr=1
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
