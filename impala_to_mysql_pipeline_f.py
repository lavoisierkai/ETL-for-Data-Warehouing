#!/usr/bin/env python
# coding: utf-8
# author: kai

import impala
import csv
import pandas as pd
import numpy as np
from impala.dbapi import connect
import datetime
# from datetime import datetime
import pymysql as psl

# print(datetime.datetime.now(tz=None))
filtimestamp = str(datetime.datetime.now(tz=None)).replace(" ", "_").replace(".","_").replace("-","_").replace(":","_")
filename = "rolling_forcast"+"_"+filtimestamp
print(filename)

impyla_host = 'XXXX.XXX.XXX'
impyla_port = 99999
impyla_user = 'XXXXX'
impala_password = 'XXXXXX'


""" 
impala connection
"""
def get_impyla(querry):
    conn = connect(host=impyla_host, port=impyla_port, user=impyla_user, password=impala_password) #修改hive链接信息
    cur = conn.cursor()
    cur.execute(querry)
    dblist=cur.fetchall()
    dbtitle=cur.description
    titles=[]
    if len(dbtitle)>0:
        for column in range(0, len(dbtitle)):
            titles.append(dbtitle[column][0])
    df = pd.DataFrame(list(dblist), columns = titles)
    col=[0]*df.shape[1]
    for i in range(df.shape[1]):
        try:
            col[i]=df.columns[i][
                df.columns[i].index('.')+1:]
        except:
            col[i]=df.columns[i]
    df.columns=col
    conn.close()
    return df



if __name__ == "__main__":

    querry_impyla = '''
    select * from ****** where dt = '2020-10-01' limit 100;
    '''
    impala_df = get_impyla(querry_impyla)

    impala_df.to_csv(r"./log/{filename}.txt".format(filename = filename), encoding='utf-8-sig', header=None, index=None, sep='\t', mode='a')


