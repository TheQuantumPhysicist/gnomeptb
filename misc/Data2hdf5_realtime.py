import h5py
import numpy as np
import datetime as dt
import glob
import os
import time
import re
#import comb_utils 

# changes: Use GPS time, since 2015-07-01 17s ahead of UTC
dtGPS_UTC = dt.timedelta(seconds=17)


combpath = '.\\Comb\\'
cavitiespath = '.\\Cavities\\'
hdf5path = '.\\hdf5\\'

maxcol = 4
sync = False
first= True  # first file?

f123=np.ones((60000,2), dtype=np.float64)



def hd5_open(fname):    # TODO: use real data from file
    f = h5py.File(hdf5path + fname, "w")
    f.attrs['Content'] = "frequency differences of two vertical optical silicon cavities (f1, f2) and one horizontal ULE cavity (f3) all at 1542 nm"
        
    dset = f.create_dataset("CavityData", (60000,2), dtype='float64', compression="gzip", compression_opts=9)
    
    dset.attrs['Altitude'] = 78
    dset.attrs['ChannelRange'] = "4 kHz - 65 MHz"
    dset.attrs['Date'] = "2016/05/02"
    # silicon cavities  (from google maps)
    dset.attrs['Latitude'] = 52.296052
    dset.attrs['Longitude'] = 10.461654
    dset.attrs['MissingPoints'] = 0
    dset.attrs['SamplingRate(Hz)'] = 1000
    dset.attrs['Units'] = "Hz"
    dset.attrs['t0'] = "11:38:00.000000"
    dset.attrs['t1'] = "11:39:00.000000"

    dset.attrs['Altitude ULE'] = 77
    # Sr beast (from google maps)
    dset.attrs['Latitude ULE'] = 52.296351
    dset.attrs['Longitude ULE'] = 10.461555
 
    return f


def hd5_write_comb(hdf, UTC0, UTC1):
    dset = hdf.create_dataset('CombData', (60,1), dtype='float64', compression='gzip', compression_opts=9)
    dset.attrs['Altitude'] = 78
    dset.attrs['ChannelRange'] = '4 kHz - 65 MHz'
    dset.attrs['Date'] = "2016/05/02"
    # silicon cavities  (from google maps)
    dset.attrs['Latitude'] = 52.296052
    dset.attrs['Longitude'] = 10.461654
    dset.attrs['MissingPoints'] = 0
    dset.attrs['SamplingRate(Hz)'] = 1
    dset.attrs['Units'] = 'Hz'
    dset.attrs['t0'] = (UTC0+dtGPS_UTC).strftime('%H:%M:%S.000')
    dset.attrs['t1'] = (UTC1+dtGPS_UTC).strftime('%H:%M:%S.000')
    
    #data=comb_utils.loadKK([fncomb], ['CEO','Si1'], average=1, d0=UTC0, d1=UTC1)
    
    fc = np.ones(60, dtype=np.float64)
    for i in range(60):
        #hdf['CombData'][i] = data['Si1'][i]
        hdf['CombData'][i] = 123000.0 + i
    return

def tail_line(file, path):        # read last line, closes file and returns, if file is no longer the newest one
    interval = 0.2
    line = file.readline()
    while True:
        where = file.tell()
        line = file.readline()
        if not line or line[-1]!='\n':
            time.sleep(1e-3)
            if file.name!=max(glob.iglob(path+'*Frequ.txt'), key=os.path.getmtime):  # found newer file
                file.close()
                print ('found newer data file')
                return
            file.seek(where)
        else:
            yield line
            
            
###################### main ########################  
#fname='160324_1_Frequ.txt.part0.txt'

fncomb     = max(glob.iglob(combpath+'*Frequ.txt'), key=os.path.getmtime)
fncavities = max(glob.iglob(cavitiespath+'*Frequ.txt'), key=os.path.getmtime)
 
#fcomb = open(fncomb,'r')
fcavities = open(fncavities,'r')

iline = 0
isync = 0
nsync = 0
skiplines=0  # skip 

rcomb = re.compile(r"(?P<date>\d{6})(?P<sync>.)(?P<time>\d+\.\d+) +(?P<bin>\w{8}) +(?P<f1>\d+\.\d+) +(?P<f2>\d+\.\d+) +(?P<f3>\d+\.\d+) +"
    "(?P<f4>\d+\.\d+) +(?P<f5>\d+\.\d+) +(?P<f6>\d+\.\d+)")
    
rcavities = re.compile(r"(?P<date>\d{6})(?P<sync>.)(?P<time>\d+\.\d+) +(?P<f1>\d+\.\d+) +(?P<f2>\d+\.\d+) +(?P<f3>\d+\.\d+)")
    
    
while True:
    newest = max(glob.iglob(cavitiespath+'*.txt'), key=os.path.getmtime)
    fcomb = open(newest)
    print(fcomb)
    # start at first line    

    for line in tail_line(fcavities, cavitiespath):
        iline = iline+1
        
        m1 = rcavities.match(line)
        if m1 == None:
            continue
    
        if m1.group('sync')=='*': 
            if not sync:   # first sync event
                #print 'first sync'
                isync = iline
                sync=True
            nsync = nsync+1
            #print ' sync'
            newsync = True
        else:
            newsync = False
        
        dUTC = dt.datetime.strptime(m1.group('date')+m1.group('time'),'%y%m%d%H%M%S.%f')
        dGPS = dUTC + dtGPS_UTC     # GPS-UTC offset since 2015-07-01
        #d += 1721424.5 - 2400000.5 
        #d += (float(inline[1][0:2])+(float(inline[1][2:4])+float(inline[1][4:])/60)/60)/24
        #ti = tti + inline[1][0:2] + ':' + inline[1][2:4] + ':' + inline[1][4:6] + '.00'  ## timestamps are at UTC seconds
   
        if newsync and dGPS.second==0:
            if not first:   # data was written already
                dUTC1 = dUTC
                f123.attrs['t1'] = dGPS.strftime('%H:%M:%S.000')
                hd5_write_comb(hdf, dUTC0, dUTC1)
                hdf.close()
 
            dUTC0 = dUTC
                
            #print line
            hd5name='PTB01_' + dGPS.strftime('%Y%m%d_%H%M%S') +'.hdf5'
            print('HDF5 Filename: ', hd5name)
            hdf = hd5_open(hd5name)
            first=False
            f123 = hdf['CavityData']
            f123.attrs['t0'] = dGPS.strftime('%H:%M:%S.000')
            Date = '20' + m1.group('date')[0:2] + '/' + m1.group('date')[2:4] + '/' + m1.group('date')[4:6] # "2016/05/02"
            f123.attrs['Date'] = Date
        
        
            if (iline-isync)%60000>0:
                #print dGPS.strftime('%H:%M:%S.000   '), iline 
                #break
                pass
    
        f123[(iline-isync-1)%60000,0] = float(m1.group('f1'))
        f123[(iline-isync-1)%60000,1] = float(m1.group('f2'))
    
        # some diagnostics
        if nsync%60==1: # every minute
            #print dUTC 
            pass
        ddt = float(inline[1][4:])
        
        if nsync>100000: # TEST: stop after some time
            break
    
    if not first:
        hdf.close()
    