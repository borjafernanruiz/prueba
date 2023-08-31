import image_slicer
import os
import re
import numpy as np
from PIL import Image
import argparse
import shutil
import time


import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait, ALL_COMPLETED

from patchify import patchify

import pymongo

# mongo_client=pymongo.MongoClient("mongodb://{}:{}/".format('localhost','27017'))
# mongo_colection=mongo_client['map_purple_tiles'][str(t1)+'_'+str(t2)]


#Input of the script
parser = argparse.ArgumentParser()
parser.add_argument('--folder_data',  type=str,required=True,  help='Folder where the CSV files of the data are located')
# parser.add_argument('--zoom',  type=int,required=True,  help='Zoom level')
parser.add_argument('--output', '--out',  type=str,required=True,  help='Filename where you want to save the data')
args = parser.parse_args()

folder = args.folder_data
# zoom = args.zoom
output = args.output
# os.mkdir(output)

contenido = os.listdir(folder)

a=time.time()

def Add_zero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)


mongo_client=pymongo.MongoClient("mongodb://{}:{}/".format('localhost','27017'))
query = {"zoom":9} 
contenido_not_purples=set(contenido)
for i in range(0,16):
    for ii in range(0,16):
        mongo_colection=mongo_client['map_purple_tiles'][str(i)+'_'+str(ii)]
        purples=list(mongo_colection.find(query,{"_id":0,"zoom":0}))
        purples=['h'+Add_zero(i['h'])+'v'+Add_zero(i['v'])+'.png' for i in purples]
        contenido_not_purples=contenido_not_purples-set(purples)
contenido_not_purples=list(contenido_not_purples)


def Dividir(tesela,zoom,out):
    I=Image.open(folder+'\\'+tesela)
    exp=2**(zoom-9)
    d=256/exp
    patches = patchify(np.array(I), (d,d,4), step=int(d))
    v=re.split('v|h|_|.p', tesela)
    for i in range(0,exp):
        for ii in range(0,exp):
            subimage=patches[ii,i,:,:][0]
            if (0,0,0,0)!=(subimage[:,:,0].std(),subimage[:,:,1].std(),subimage[:,:,2].std(),subimage[:,:,3].std()):
                newname='h'+str(int(v[1])*exp+i)+'v'+str(int(v[2])*exp+ii)+'.png'
                image=Image.fromarray(subimage) 
                image = image.resize((256,256), Image.NEAREST)
                image.save(out+'\\'+newname)

def Dividir_V(V):
    Dividir(V[0],V[1],V[2])

print(len(contenido_not_purples))

def Paralelizar(zoom,contenido_not_purples):
    out=output+'\\'+str(zoom)
    os.mkdir(out)
    inputs=[(i,zoom,out) for i in contenido_not_purples]
    with ThreadPoolExecutor() as executor1:
        executor1.map(Dividir_V,inputs)
for i in range(10,15):
    Paralelizar(i,contenido_not_purples)
    print('zoom:')
    print(i)

print(time.time()-a)





