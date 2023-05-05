#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  5 18:13:19 2023

@author: carlosdm

Programa paralelo que calcula los 3-ciclos de un grafo que es la unión de los grafos 
definido como lista de aristas en más de un fichero de texto.

"""
from pyspark import SparkContext
import sys


def get_edges(line): # grafo no dirigido, sin bucles.
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2
    
def adyac_existe_falta(nodo_con_adyac):
    tri=[]
    nodo=nodo_con_adyac[0]
    conexiones=nodo_con_adyac[1]
    n = len(conexiones)
    for i in range(n):
        conn=conexiones[i]
        tri.append(((nodo,conn),'existe'))
        for j in range(i+1, len(conexiones)):
            tri.append(((conn,conexiones[j]),('falta', nodo)))
    return tri


def main(sc, in_files_list): 
    final_tri=[]
    graf=sc.emptyRDD()
    for file in in_files_list:
        graf = graf.union(sc.textFile(file)) 
    # de esta forma juntamos los rdd de cada archivo en uno solo
    graf_leido=graf.map(get_edges).filter(lambda x: x is not None).distinct()
    adyac=graf_leido.groupByKey().map(lambda x: (x[0],sorted(list(x[1])))).sortByKey()
    tri_list=adyac.flatMap(adyac_existe_falta).groupByKey()
    for conn, estados in tri_list.collect():
        s_list=list(estados)
        if len(s_list)>=2  and bool(list(filter(lambda x: x == 'existe', s_list))): 
            for k in s_list:
                if k!='existe': 
                    final_tri.append((k[1],conn[0],conn[1]))
    print("Total de triciclos: ", len(final_tri))
    print("conexiones de triciclos", final_tri)
    

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("ERROR: se necesitan 2 o más ficheros del tipo .txt como parámetros")
    else:
        with SparkContext() as sc:
           sc.setLogLevel("ERROR")
           main(sc, sys.argv[1:])

