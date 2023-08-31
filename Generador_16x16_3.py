import numpy as np
import pandas as pd
import argparse
from PIL import Image
from skimage import io
import os
import branca
import time
import json
import pymongo
import h5py     

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait, ALL_COMPLETED



#Input of the script
parser = argparse.ArgumentParser()
parser.add_argument('--folder_data',  type=str,required=True,  help='Folder where the CSV files of the data are located')
parser.add_argument('--tile',  type=str,  help='Tile pisition. Format example: [1,2]')
parser.add_argument('--mongoDB',  type=str,  help='If saving data in MongoDB')
parser.add_argument('--dataset_save',  type=str,  help='If saving data in CSV dataset')
parser.add_argument('--output', '--out',  type=str,required=True,  help='Filename where you want to save the data')
args = parser.parse_args()

folder = args.folder_data
tile_arg=args.tile
mongoDB=args.mongoDB
dataset_save=args.dataset_save

if mongoDB==None:
    mongoDB=True
elif mongoDB.lower()=='false':
    mongoDB=False
elif mongoDB.lower()=='true':
    mongoDB=True
else:
    print('ERROR mongoDB input')

if dataset_save==None:
    dataset_save=True
elif dataset_save.lower()=='false':
    dataset_save=False
elif dataset_save.lower()=='true':
    dataset_save=True
else:
    print('ERROR dataset_save input')

if tile_arg:
    tile=np.array(tile_arg[1:-1].split(',')).astype('int')
    x_tile = tile[0]
    y_tile = tile[1]

zoom=4
output = args.output

# De radiación a magnitudes
def radiacion_a_mag(rad):
    a=np.round(np.log10(rad)*-0.95+20.93,2)
    a[rad==0]=22
    a[a>22]=22
    return a 

def grado_a_radianes(alfa):
    return alfa*2*np.pi/360

def radianes_a_grado(alfa):
    return alfa*360/(2*np.pi)

def equirectangular_to_mercator(longitude,latitude,zoom):
    longitude=grado_a_radianes(longitude)
    latitude=grado_a_radianes(latitude)

    x=256*2**zoom*(np.pi+longitude)/(2*np.pi)
    y=256*2**zoom*(np.pi-np.log(np.tan(np.pi/4+latitude/2)))/(2*np.pi)
    return x,y

def mercator_to_equirectangular(x,y,zoom):
    longitude=2*np.pi*x/(256*2**zoom)-np.pi
    latitude=2*np.arctan(np.exp(np.pi-2*np.pi*y/(256*2**zoom)))-np.pi/2

    return radianes_a_grado(longitude),radianes_a_grado(latitude)

def Tile(V): #Obtiene la tesela de equirectangular en la que se encuentran unas coordenadas
    lon=V[0]
    lat=V[1]
    v=np.floor((90-lat)/10)
    h=np.floor((lon+180)/10)
    return int(v),int(h)

def Esquina_superior_derecha(V): #Esquina superior derecha de una tesela equirectangular
    lat=90-V[1]*10
    lon=V[0]*10-180
    return lon,lat

def Add_zero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)
    
def Datos_tesela(x,y,zoom): #Listado de teselas equireactangular necesarias para una tesela mercator
    esquina_izq_sup=Tile(mercator_to_equirectangular(x*256,y*256,zoom))
    esquina_der_inf=Tile(mercator_to_equirectangular(256*(x+1),256*(y+1),zoom))
    V=[]
    for i in range(esquina_izq_sup[0],esquina_der_inf[0]+1):
        for ii in range(esquina_izq_sup[1],esquina_der_inf[1]+1):
            V=V+['h'+Add_zero(ii)+'v'+Add_zero(i)]
    return V

def Nombre_a_tesela(name):
    return int(name[1:3]),int(name[4:])

def Degree_decimal_to_degree_hexadecimal(degree_decimal): #Borja
            degree=np.floor(degree_decimal).astype('int') #Borja
            minute_decimal=(degree_decimal-degree)*60 #Borja
            minute=np.floor(minute_decimal).astype('int') #Borja
            second=np.floor((minute_decimal-minute)*60).astype('int') #Borja
            return (degree,minute,second) #Borja

def Generar_tesela(t1,t2,zoom):
    out=output+"\mapa"+"\\CSV\\"
    os.makedirs(out, exist_ok=True)
    DF=pd.DataFrame()
    archivos=os.listdir(folder)
    archivos2=np.array([i.split('.')[2] for i in archivos])

    for i in Datos_tesela(t1,t2,zoom):
        p=np.where(archivos2==i)[0]
        try:
            Data=pd.DataFrame()
            h5file = h5py.File(folder+"\\"+archivos[p[0]],"r")
            var1=np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields']['AllAngle_Composite_Snow_Free'])
            var2=np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields']['lat'])
            var3=np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields']['lon'])

            Data['AllAngle_Composite_Snow_Free']=var1.reshape(1,-1)[0]
            Data['lat']=list(var2.reshape(1,-1)[0])*len(var3)
            Data['lon']=np.array([[i]*len(var2) for i in var3.reshape(1,-1)[0]]).reshape(1,-1)[0]
            Data['AllAngle_Composite_Snow_Free']=Data['AllAngle_Composite_Snow_Free'].replace({65535:np.nan})*0.1 #Cambio nulos
            Data=Data.sort_values(['lat','lon'],ascending=[False,True])
        except:
            Data=pd.DataFrame()
            Data['AllAngle_Composite_Snow_Free']=[np.nan]*5760000


        Esquina_sup_der=Esquina_superior_derecha(Nombre_a_tesela(i))
        LON=np.linspace(Esquina_sup_der[0],Esquina_sup_der[0]+10,2401)[:-1]
        LAT=np.linspace(Esquina_sup_der[1],Esquina_sup_der[1]-10,2401)[:-1]
        LON=np.tile(LON, (1, 2400))[0]
        LAT=np.tile(LAT, (1, 2400))[0]
        #LAT=np.transpose(LAT.reshape(2400,2400)).reshape([-1])
        LON=np.transpose(LON.reshape(2400,2400)).reshape([-1])
        Data['lat']=LAT
        Data['lon']=LON
        (X,Y)=equirectangular_to_mercator(Data['lon'].values,Data['lat'].values,zoom)
        Data['X']=X
        Data['Y']=Y
        Data=Data[(Data['X']>=t1*256) & (Data['Y']>=t2*256)]
        Data=Data[(Data['X']<(t1+1)*256) & (Data['Y']<(t2+1)*256)]
        Data=Data[(Data['Y']>=0)]
          
        DF=pd.concat([DF,Data])

    #print(DF[DF['AllAngle_Composite_Snow_Free']>0]['AllAngle_Composite_Snow_Free'].min())
    DF['mag']=radiacion_a_mag(DF['AllAngle_Composite_Snow_Free'])

    if dataset_save:
        DF[['mag','X','Y']].to_csv(out+"\h"+Add_zero(t1)+"v"+Add_zero(t2)+".csv", sep=';',index=False)

    if mongoDB:

        DF2=DF[DF['mag']>0][['mag','lon','lat']]

        Degree_hex_LON=Degree_decimal_to_degree_hexadecimal(DF2['lon'])
        Degree_hex_LAT=Degree_decimal_to_degree_hexadecimal(DF2['lat'])
        DF2['grad_lon']=Degree_hex_LON[0]
        DF2['min_lon']=Degree_hex_LON[1]
        DF2['sec_lon']=(np.round(Degree_hex_LON[2]/15)*15).astype('int')
        DF2['grad_lat']=Degree_hex_LAT[0]
        DF2['min_lat']=Degree_hex_LAT[1]
        DF2['sec_lat']=(np.round(Degree_hex_LAT[2]/15)*15).astype('int')
        DF2['min_lon']=DF2['min_lon']+(DF2['sec_lon']==60).astype('int')
        DF2['sec_lon']=DF2['sec_lon']*(DF2['sec_lon']!=60).astype('int')
        DF2['grad_lon']=DF2['grad_lon']+(DF2['min_lon']==60).astype('int')
        DF2['min_lon']=DF2['min_lon']*(DF2['min_lon']!=60).astype('int')
        DF2['min_lat']=DF2['min_lat']+(DF2['sec_lat']==60).astype('int')
        DF2['sec_lat']=DF2['sec_lat']*(DF2['sec_lat']!=60).astype('int')
        DF2['grad_lat']=DF2['grad_lat']+(DF2['min_lat']==60).astype('int')
        DF2['min_lat']=DF2['min_lat']*(DF2['min_lat']!=60).astype('int')
        DF2=DF2[['mag','grad_lon','min_lon','sec_lon','grad_lat','min_lat','sec_lat']]

        dic=DF2.to_dict(orient = 'records')
        mongo_client=pymongo.MongoClient("mongodb://{}:{}/".format('localhost','27017'))
        mongo_colection=mongo_client['map_values'][str(t1)+'_'+str(t2)]
        mongo_colection.insert_many(dic)
        mongo_colection.create_index([("grad_lat", pymongo.DESCENDING),("grad_lon", pymongo.DESCENDING),("min_lat", pymongo.DESCENDING),("min_lon", pymongo.DESCENDING),("sec_lat", pymongo.DESCENDING),("sec_lon", pymongo.DESCENDING)], unique=True)

def Generar_tesela_V(V):
    Generar_tesela(V[0],V[1],V[2])



inicio = time.time()
if tile_arg:
    Generar_tesela(x_tile,y_tile,zoom)
else:
    contenido=[]
    for i in range(0,2**zoom):
        for ii in range(0,2**zoom):
            contenido=contenido+[(i,ii,zoom)]
    with ThreadPoolExecutor() as executor1:
        executor1.map(Generar_tesela_V,contenido)
    # cont=0
    # for i in range(0,2**zoom):
    #     for ii in range(0,2**zoom):
    #         Generar_tesela(i,ii,zoom)
    #         cont=cont+1
    #         print(str(np.round(cont/(2**(2*zoom))*100,2))+'%')
print('He acabao')
fin = time.time()
print('T')
print(fin-inicio)


