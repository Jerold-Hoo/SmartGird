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


def plot(resID,engine):
    SQLstr = ''.join(["select * from A_resAll where ResID = ",str(resID)," order by begintime"])
    df = pd.read_sql(SQLstr,engine)

    ax1 = plt.subplot(311)
    ax2 = plt.subplot(312)
    ax3 = plt.subplot(313)
    ax1.plot(df['begintime'], df['real_gene'])
    ax1.plot(df['begintime'], df['real_load'])
    ax1.plot(df['begintime'], df['charge'])
    ax1.plot(df['begintime'], df['discharge'])
    ax1.plot(df['begintime'], df['buy'])
    ax1.plot(df['begintime'], df['sale'])
    ax1.legend(['real_gene', 'real_load', 'charge', 'discharge', 'buy', 'sale'])

    ax2.plot(df['begintime'], df['planbuy'])
    ax2.plot(df['begintime'], df['plansale'])
    ax2.legend(['planbuy', 'plansale'])

    ax3.plot(df['begintime'], df['price_buy'])
    ax3.plot(df['begintime'], df['price_sale'])
    ax3.plot(df['begintime'], df['planprice'])
    ax3.legend(['price_buy','price_sale','planprice'])
    plt.show()

def test(engine):
    import Para
    pt = Para.Para_PriceTable('2018-10-01',engine)
    print(pt.buy_table)
    print(pt.sale_table)
    pt.refrash(datetime(2018,10,1,10))
    print(pt.buy_table)
    print(pt.sale_table)

    #a = pt.plandf[(pt.plandf['BeginTime'] >= datetime(2018,10,1,4,15)) & (pt.plandf['BeginTime'] <= datetime(2018,10,1,6,15))]

    #print(a)
    #print(pt.get_plan(datetime(2018,10,1,4,15),datetime(2018,10,1,8,45)))
    #res = pt.get_Idxs_allhigher_aft(datetime(2018,2,4),datetime(2018,12,5))
    #res = pt.get_Idxs_buyhigher_aft(datetime(2018, 2, 4), datetime(2018, 2, 5))
    #for i in res:
       #print(i.begintime,i.id,i.price,i.ptype)

if __name__ == '__main__':

    engine = sql.create_engine('mssql+pyodbc://sa:2019newpass.7142sql@118.123.7.142:1433/GIRD_EN?driver=SQL+Server')
    #import Para

    #dayplan = Para.Para_DayAheadPlan('2018-10-01',engine)
    #plan = dayplan.get_plan(datetime(2018,10,1,17), datetime(2018,10,2))
    #print(plan)

    res = MA.gird_model('test','2018-10-01',15,engine,priceRatio=1,geneRatio=1,loadRatio=1)
    #print(res)
    res.to_sql('A_res',engine,if_exists='replace')

    #plot(2,engine)
    #test(engine)


