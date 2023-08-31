import pandas as pd
import numpy as np 
import h5py     
from shutil import rmtree
import argparse
import os
from PIL import Image
from skimage import io

parser = argparse.ArgumentParser()
parser.add_argument('--out','--output', required=True, type=str, help='Output filename')
parser.add_argument('--year',  required=True, type=int, help='Year')
parser.add_argument('--token', required=True, type=str, help='NASA EARTHDATA token. Please visit the link https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A4/2021/001/. If necessary, register an account. It is important to have an active user account to proceed. Click on -See wget Download command- to obtain the token. If there is not a token, download a file and click on it again. The token expires every 4-6 months.')
parser.add_argument('--OS',required=True,help='Operating system: linux ot windows')

args = parser.parse_args()

output=args.out
ano=args.year
token=args.token
OS=args.OS


#FUNCIONES DE DESCARGA OBTENIDAS DE LA PÇAGINA DE LA NASA MODIFICADAS PARA QUE DESCARGUEN SÓLO CIERTOS ARCHIVOS Y NO LA TOTALIDAD COMO INDICA LA NASA
#from __future__ import (division, print_function, absolute_import, unicode_literals)
import argparse
import os
import os.path
import shutil
import sys
from io import StringIO 

USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')


def geturl(url, token=None, out=None):
    headers = { 'user-agent' : USERAGENT }
    if not token is None:
        headers['Authorization'] = 'Bearer ' + token
    try:
        import ssl
        CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        if sys.version_info.major == 2:
            import urllib2
            try:
                fh = urllib2.urlopen(urllib2.Request(url, headers=headers), context=CTX)
                if out is None:
                    return fh.read()
                else:
                    shutil.copyfileobj(fh, out)
            except urllib2.HTTPError as e:
                print('HTTP GET error code: %d' % e.code(), file=sys.stderr)
                print('HTTP GET error message: %s' % e.message, file=sys.stderr)
            except urllib2.URLError as e:
                print('Failed to make request: %s' % e.reason, file=sys.stderr)
            return None

        else:
            from urllib.request import urlopen, Request, URLError, HTTPError
            try:
                fh = urlopen(Request(url, headers=headers), context=CTX)
                print(fh)
                if out is None:
                    return fh.read().decode('utf-8')
                else:
                    shutil.copyfileobj(fh, out)
            except HTTPError as e:
                print('HTTP GET error code: %d' % e.code(), file=sys.stderr)
                print('HTTP GET error message: %s' % e.message, file=sys.stderr)
            except URLError as e:
                print('Failed to make request: %s' % e.reason, file=sys.stderr)
            return None

    except AttributeError:
        # OS X Python 2 and 3 don't support tlsv1.1+ therefore... curl
        import subprocess
        try:
            args = ['curl', '--fail', '-sS', '-L', '--get', url]
            for (k,v) in headers.items():
                args.extend(['-H', ': '.join([k, v])])
            if out is None:
                # python3's subprocess.check_output returns stdout as a byte string
                result = subprocess.check_output(args)
                return result.decode('utf-8') if isinstance(result, bytes) else result
            else:
                subprocess.call(args, stdout=out)
        except subprocess.CalledProcessError as e:
            print('curl GET error message: %' + (e.message if hasattr(e, 'message') else e.output), file=sys.stderr)
        return None

def sync(src, dest, tok,A):
    '''synchronize src url with dest directory'''
    try:
        import csv
        files = [ f for f in csv.DictReader(StringIO(geturl('%s.csv' % src, tok)), skipinitialspace=True) ]
    except ImportError:
        import json
        files = json.loads(geturl(src + '.json', tok))

    # use os.path since python 2/3 both support it while pathlib is 3.4+
    for f in files:
        print(f['name'])
        if f['name'] in A:
          # currently we use filesize of 0 to indicate directory
          filesize = int(f['size'])
          path = os.path.join(dest, f['name'])
          url = src + '/' + f['name']
          if filesize == 0:
              try:
                  print('creating dir:', path)
                  os.mkdir(path)
                  sync(src + '/' + f['name'], path, tok)
              except IOError as e:
                  print("mkdir `%s': %s" % (e.filename, e.strerror), file=sys.stderr)
                  sys.exit(-1)
          else:
              try:
                  if not os.path.exists(path):
                      print('downloading: ' , path)
                      with open(path, 'w+b') as fh:
                          geturl(url, tok, fh)
                  else:
                      print('skipping: ', path)
              except IOError as e:
                  print("open `%s': %s" % (e.filename, e.strerror), file=sys.stderr)
                  sys.exit(-1)
    return 0

#FIN DE FUNCIONES NASA
#TOKEN de mi usuario
#token="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJBUFMgT0F1dGgyIEF1dGhlbnRpY2F0b3IiLCJpYXQiOjE2ODUwMTIyNTcsIm5iZiI6MTY4NTAxMjI1NywiZXhwIjoxNzAwNTY0MjU3LCJ1aWQiOiJib3JqYWZlcm5hbnJ1aXoiLCJlbWFpbF9hZGRyZXNzIjoiYm9yamFmZXJuYW5ydWl6QGdtYWlsLmNvbSIsInRva2VuQ3JlYXRvciI6ImJvcmphZmVybmFucnVpeiJ9.WgWKMEupUp-GxXkAD3qOB8IK69pqMiZAaSlyktDA6tM"


def Poner_cero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)
    
#Formato de los días con los 1->001, 23->023 0 124->124
def dia_format(dia):
    if dia<10:
        return '00'+str(dia)
    elif dia<100:
        return '0'+str(dia)
    else:
        return str(dia)
    
cuadrantes=[]
for i in range(0,18):
    for ii in range(0,36):
        cuadrantes=cuadrantes+['h'+Poner_cero(ii)+'v'+Poner_cero(i)]    


def Nombre(ano,dia,producto):
    dia=dia_format(dia)
    csv=pd.read_csv('https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/'+producto+'/'+str(ano)+'/'+dia+'.csv')
    archivos=[(None if i==[] else i[0]) for i in [[i for i in csv['name'] if ii in i] for ii in cuadrantes]]
    return archivos


def Descarga(ano,dia,producto):
    A=set(Nombre(ano,dia,producto))
    url='https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/'+producto+'/'+str(ano)+'/'+dia_format(dia) 
    #out=r'/mnt/data/datos_borja/VIIRS/Diarios_3/ano_'+str(ano)+'/dia_'+dia_format(dia)
    if OS=='linux':
        out=output+'/ano_'+str(ano)+'/dia_'+dia_format(dia)
    elif OS=='windows':
        out=output+'\\ano_'+str(ano)+'\\dia_'+dia_format(dia)
    else:
        print('ERROR in OS input')
    os.makedirs(out, exist_ok=True)
    sync(url,out,token,A)

# De radiación a magnitudes
def radiacion_a_mag(rad):
    a=np.round(np.log10(rad)*-0.95+20.93,2) #CAMBIAR EN CASO DE CAMBIO DE AJUSTE
    a[rad==0]=22
    a[a>22]=22
    return a

# De magnitudes a bits
def mag_a_bits(mag):
    a=np.round((mag-14)*20)
    a[np.isnan(a)]=255
    return a.astype(np.uint8) 

def Descarga_TOTAL(ano,dia):
    Descarga(ano,dia,'VNP46A4')
    carpeta=output+'/ano_'+str(ano)+'/dia_'+dia_format(dia)
    archivos=os.listdir(carpeta)
    #DATA=pd.DataFrame()
    cont=1
    for ii in archivos:
        #if cont>1:
            #DATA=pd.read_csv(output+"/Datos_mapa_global.csv")
        print(ii)

        Data=pd.DataFrame()
        h5file = h5py.File(carpeta+"/"+ii,"r")
        var1=np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields']['AllAngle_Composite_Snow_Free'])
        var2=np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields']['lat'])
        var3=np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields']['lon'])

        Data['AllAngle_Composite_Snow_Free']=var1.reshape(1,-1)[0]
        Data['lat']=list(var2.reshape(1,-1)[0])*len(var3)
        Data['lon']=np.array([[i]*len(var2) for i in var3.reshape(1,-1)[0]]).reshape(1,-1)[0]

        Data['AllAngle_Composite_Snow_Free']=Data['AllAngle_Composite_Snow_Free'].replace({65535:np.nan})*0.1 #Cambio nulos

        n_lat=len(set(Data['lat']))
        n_lon=len(set(Data['lon']))
        print(n_lat,n_lat)

        matrix=Data['AllAngle_Composite_Snow_Free'].values.reshape(n_lat,n_lon)
        matrix=mag_a_bits(radiacion_a_mag(matrix))

        codigo=ii.split('.')[2]
        im = Image.fromarray(matrix)
        im = im.convert("L")
        try:
            im.save(output+'/teselas/'+codigo+'.png')
        except:
            os.mkdir(output+'/teselas')
            im.save(output+'/teselas/'+codigo+'.png')
        cont=cont+1
        print(str(np.round(cont/len(archivos)*100,2))+'%')
        #print(str(np.round),2)+'%')
    #rmtree(carpeta)
        

Descarga_TOTAL(ano,1)





