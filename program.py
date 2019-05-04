# -*- coding: utf-8 -*-

# @File       : program.py
# @Date       : 2019-04-26
# @Author     : Jerold
# @Description:

import pandas as pd
from datetime import *
import sqlalchemy as sql
import matplotlib.pyplot as plt
import ModelAlgo as MA

def plot(name,engine):
    SQLstr = ''.join(["select * from A_resAll where name = '",name,"' order by begintime"])
    df = pd.read_sql(SQLstr,engine)

    ax1 = plt.subplot(311)
    ax2 = plt.subplot(312)
    ax3 = plt.subplot(313)
    ax1.plot(df['begintime'], df['real_gene'])
    ax1.plot(df['begintime'], df['real_load'])
    ax1.plot(df['begintime'], df['charge_from_gene'] + df['charge_from_buy'])
    ax1.plot(df['begintime'], df['charge_from_buy'] + df['buy_to_load'])
    #ax1.plot(df['begintime'], df['buy_to_load'])
    #ax1.plot(df['begintime'], df['gene_to_load'])
    #ax1.plot(df['begintime'], df['storage_KWH'])
    ax1.legend(['real_gene', 'real_load', 'charge', 'buy'])

    ax2.plot(df['begintime'], df['price_buy'])
    ax2.plot(df['begintime'], df['price_buyOri'])
    #ax2.plot(df['begintime'], df['price_buy_std'])
    ax2.legend(['All_buy_UseMoney', 'Ori_buy_UseMoney'])

    ax3.plot(df['begintime'], df['price_buy_std'])
    ax3.legend(['price'])
    plt.show()

def test(engine):
    import Para
    pt = Para.Para_DayAheadPlan('2018-10-01',engine)
    #a = pt.plandf[(pt.plandf['BeginTime'] >= datetime(2018,10,1,4,15)) & (pt.plandf['BeginTime'] <= datetime(2018,10,1,6,15))]

    #print(a)
    #print(pt.get_plan(datetime(2018,10,1,4,15),datetime(2018,10,1,8,45)))
    #res = pt.get_Idxs_allhigher_aft(datetime(2018,2,4),datetime(2018,12,5))
    #res = pt.get_Idxs_buyhigher_aft(datetime(2018, 2, 4), datetime(2018, 2, 5))
    #for i in res:
       #print(i.begintime,i.id,i.price,i.ptype)

if __name__ == '__main__':

    engine = sql.create_engine('mssql+pyodbc://sa:2019newpass.7142sql@118.123.7.142:1433/GIRD_EN?driver=SQL+Server')

    res = MA.gird_model('test','2018-10-01',15,engine)
    #res = MA.gird_model('Eng','2018-02-03',30,engine)
    print(res)
    res.to_sql('A_res',engine,if_exists='replace')

    #plot('test:MAX=1000,SPEED=200,INIT=0,CRate=95,DisCRate=95',engine)
    #test(engine)


