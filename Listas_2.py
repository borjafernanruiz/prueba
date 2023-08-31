# python Re_teselado_7.py --folder_data C:\Users\borja\Downloads\mapa\CSV --zoom 1 --output C:\Users\borja\Downloads\V1 --left_upper_coordinate [29.21569,-18.30391] --right_lower_coordinate [27.56400,-13.27821]
import numpy as np
import pandas as pd
import argparse
from PIL import Image
from skimage import io
import os
import branca
import time
import pymongo

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor


#Input of the script
parser = argparse.ArgumentParser()
parser.add_argument('--folder_data',  type=str,required=True,  help='Folder where the CSV files of the data are located')
parser.add_argument('--tile',  type=str,  help='Tile pisition. Format example: [1,2]')
args = parser.parse_args()

folder = args.folder_data
tile_arg=args.tile

if tile_arg:
    tile=np.array(tile_arg[1:-1].split(',')).astype('int')
    x_tile = tile[0]
    y_tile = tile[1]



def Add_zero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)
    
def Find_purples(A,zoom,t1,t2):
        B=A.copy()
        B['X']=np.floor(2**zoom*B['X']/2**4/256).astype('int')
        B['Y']=np.floor(2**zoom*B['Y']/2**4/256).astype('int')
        #print(B)
        D=B.groupby(['X','Y']).mean().reset_index()#.dropna()
        C=B.groupby(['X','Y']).std().reset_index()#.dropna()
        G=B[np.isnan(B['mag'])].groupby(['X','Y']).mean().reset_index()
        G['mag']=True
        DD=D.merge(C,left_on=['X','Y'],right_on=['X','Y'],how='left')
        DDD=DD.merge(G,left_on=['X','Y'],right_on=['X','Y'],how='left')
        DDD.columns=['X','Y','mean','std','null']
        #print(DDD)
        Morados=DDD[(DDD['mean']==22) & (DDD['null']!=True) & (DDD['std']==0 | np.isnan(DDD['std']))]
        #print(Morados)
        Vacios=DDD[np.isnan(DDD['mean'])]
        #print(Vacios)
        Resto=DDD[~((DDD['mean']==22) & (DDD['null']!=True) & (DDD['std']==0 | np.isnan(DDD['std']))|np.isnan(DDD['mean']))]
        #print(Resto)

        Morados=Morados[['X','Y']]
        Morados['zoom']=zoom
        Morados.columns=['h','v','zoom']
        dic=Morados.to_dict(orient = 'records')
        if dic!=[]:
            mongo_client=pymongo.MongoClient("mongodb://{}:{}/".format('localhost','27017'))
            mongo_colection=mongo_client['map_purple_tiles'][str(t1)+'_'+str(t2)]
            mongo_colection.insert_many(dic)
            mongo_colection.create_index([("zoom", pymongo.DESCENDING),("h", pymongo.DESCENDING),("v", pymongo.DESCENDING)], unique=True)
        
        # Resto=Resto[['X','Y']]
        # Resto['zoom']=zoom
        # Resto.columns=['h','v','zoom']
        # dic2=Resto.to_dict(orient = 'records')
        # if dic2!=[]:
        #     mongo_client=pymongo.MongoClient("mongodb://{}:{}/".format('localhost','27017'))
        #     mongo_colection=mongo_client['map_tiles'][str(t1)+'_'+str(t2)]
        #     mongo_colection.insert_many(dic2)
        #     mongo_colection.create_index([("zoom", pymongo.DESCENDING),("h", pymongo.DESCENDING),("v", pymongo.DESCENDING)], unique=True)

        

def Find_purples_all_zoom(t1,t2):
    TEXTO=folder+'\h'+Add_zero(t1)+'v'+Add_zero(t2)+'.csv'
    A=pd.read_csv(TEXTO,sep=';')
    for iii in range(5,15):
        print(iii)
        Find_purples(A,iii,t1,t2)

def Find_purples_all_zoom_V(V):
    Find_purples_all_zoom(V[0],V[1])
    

inicio = time.time()
if tile_arg:
    Find_purples_all_zoom(x_tile,y_tile)
else:
    contenido=[]
    for i in range(0,16):
        for ii in range(0,16):
            contenido=contenido+[(i,ii)]
    with ThreadPoolExecutor() as executor1:
        executor1.map(Find_purples_all_zoom_V,contenido)
print('He acabao')
fin = time.time()
print('T')
print(fin-inicio)




