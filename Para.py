# -*- coding: utf-8 -*-

# @File       : Para.py
# @Date       : 2019-04-26
# @Author     : Jerold
# @Description:

import pandas as pd
from datetime import *
from Algorithm import *
import random

class Para_PriceTable:

    def __init__(self,day,conn,frash_ratio=0.2):
        self.pp_dictbuy = {}
        self.day = day
        self.__set_buyprice(day, conn)
        self.stdbuyDF = self.buy_table.copy()
        self.__set_saleprice(day, conn)
        self.stdsaleDF = self.sale_table.copy()
        self.frash_ratio = frash_ratio
        self.conn = conn

    def __get_buypp_by_idx(self,idx,row):
        priceperiod = Para_Priceperiod(row['ID'], row['Price'], row['BeginTime'], row['EndTime'])
        """
        if self.pp_dictbuy.get(idx) is None:
            priceperiod = Para_Priceperiod(row['ID'],row['Price'],row['BeginTime'],row['EndTime'])
            self.pp_dictbuy[idx] = priceperiod
        else:
            priceperiod = self.pp_dictbuy[idx]
        """
        return priceperiod

    def __get_pp_by_betimeptype(self,row):
        priceperiod = self.pp_dictbuy.get(row['ptype']+str(row['ID']))
        if priceperiod is None:
            priceperiod = Para_Priceperiod(row['ID'],row['Price'],row['BeginTime'],row['EndTime'],row['ptype'])
            self.pp_dictbuy[row['ptype']+str(row['ID'])] = priceperiod
        return priceperiod

    def __set_buyprice(self, day, conn):
        sqlstr = ''.join(["select ID,BeginTime,EndTime,BuyPrice as Price,'buy' as ptype from Mod_PriceTableBuy where BeginTime >= '", day
                          , "' and BeginTime < dateadd(dd,1,'", day, "')"])
        self.buy_table = pd.read_sql(sqlstr, conn)

    def __set_saleprice(self, day, conn):
        sqlstr = ''.join(["select ID,BeginTime,EndTime,SalePrice as Price,'sale' as ptype from Mod_PriceTableSale where BeginTime >= '", day
                          , "' and BeginTime < dateadd(dd,1,'", day, "')"])
        self.sale_table = pd.read_sql(sqlstr, conn)

    def refrash(self,timenow,IsRandom=True):
        # 随机变化
        if IsRandom:
            for idx in self.buy_table.index:
                if self.buy_table.loc[idx, 'EndTime'] >= timenow:
                    numBuy = random.uniform(self.frash_ratio * -1,self.frash_ratio)
                    numSale = random.uniform(self.frash_ratio * -1, self.frash_ratio)
                    self.buy_table.loc[idx,'Price'] = (1 + numBuy) * self.stdbuyDF.loc[idx,'Price']
                    self.sale_table.loc[idx, 'Price'] = (1 + numSale) * self.stdsaleDF.loc[idx,'Price']
                    if self.sale_table.loc[idx, 'Price'] >= self.buy_table.loc[idx,'Price']:
                        self.sale_table.loc[idx, 'Price'] = self.buy_table.loc[idx,'Price'] * 0.95
        else:
            self.__set_buyprice(self.day, self.conn)
            self.__set_saleprice(self.day, self.conn)

        #print('price refrash:',self.sale_table[(self.sale_table['BeginTime']<=timenow) & (self.sale_table['EndTime']>timenow)].reset_index(drop=True).loc[0,'Price'])
        #print('price refrash:',self.stdsaleDF[(self.stdsaleDF['BeginTime'] <= timenow) & (self.stdsaleDF['EndTime'] > timenow)].reset_index(drop=True).loc[0, 'Price'])


    # 根据时间获取买价表时期priceperiod
    def get_PP_bytime_buy(self, time):
        for idx,row in self.buy_table.iterrows():
            if time >= row['BeginTime'] and time < row['EndTime']:
                return self.__get_buypp_by_idx('b'+str(idx),row)

    # 根据时间获取卖价表时期priceperiod
    def get_PP_bytime_sale(self, time):
        for idx,row in self.sale_table.iterrows():
            if time >= row['BeginTime'] and time < row['EndTime']:
                return self.__get_buypp_by_idx('s'+str(idx),row)

    # 找出当前时间之后、设置时间之前比自己价格高的idx list
    def get_Idxs_allhigher_aft(self, timenow, timeend=None, orderby='price desc', restype='all'):
        res_list = []
        if timeend is None:
            timeend = timenow + timedelta(days=1)
        if restype == 'all':
            df = self.buy_table.append(self.sale_table,sort='BeginTime').sort_values('BeginTime')
        elif restype == 'buy':
            df = self.buy_table
        elif restype == 'sale':
            df = self.sale_table
        print(df)
        ppNow = self.get_PP_bytime_buy(timenow)
        for idx, row in df.iterrows():
            # 在时间范围内，且价格比自己高的
            if row['BeginTime'] > timenow and row['BeginTime'] < timeend and row['Price'] > ppNow.price:
                priceperiod = self.__get_pp_by_betimeptype(row)
                in_idx = len(res_list)  # 没有找到则默认放在最后
                for i in range(len(res_list)):
                    # 等于则是时间靠后者在前
                    if priceperiod.price >= res_list[i].price:
                        in_idx = i
                        break
                res_list.insert(in_idx, priceperiod)
        return res_list

    # 找出当前时间之后、设置时间之前比自己价格高的idx list
    def get_Idxs_buyhigher_aft(self,timenow,timeend=None,orderby='price desc'):
        res_list = []
        if timeend is None:
            timeend = timenow + timedelta(days=1)

        ppNow = self.get_PP_bytime_buy(timenow)
        for idx,row in self.buy_table.iterrows():
            # 在时间范围内，且价格比自己高的
            if row['BeginTime'] > timenow and row['BeginTime'] < timeend and row['Price'] > ppNow.price:
                priceperiod = self.__get_buypp_by_idx(idx,row)
                in_idx = len(res_list) # 没有找到则默认放在最后
                for i in range(len(res_list)):
                    # 等于则是时间靠后者在前
                    if priceperiod.price >= res_list[i].price:
                        in_idx = i
                        break
                res_list.insert(in_idx,priceperiod)
        return res_list

    # 找出当前时间之后、设置时间之前比自己价格低的idx list
    def get_Idxs_buylower_aft(self,timenow,timeend=None,orderby='price desc'):
        res_list = []
        if timeend is None:
            timeend = timenow + timedelta(days=1)
        ppNow = self.get_PP_bytime_buy(timenow)
        for idx, row in self.buy_table.iterrows():
            # 满足时间，且比自己低价的时间段，等价也可以，因为比当前时间完，有优先度
            if row['BeginTime'] > timenow and row['BeginTime'] < timeend and row['Price'] <= ppNow.price:
                priceperiod = self.__get_buypp_by_idx(idx, row)
                in_idx = len(res_list) # 没有找到则默认放在最后
                for i in range(len(res_list)):
                    # 等于则是时间靠后者在前
                    if priceperiod.price <= res_list[i].price:
                        in_idx = i
                        break
                res_list.insert(in_idx, priceperiod)

        return res_list

class Para_Priceperiod:

    def __init__(self, id, price, begintime, endtime, ptype='buy'):
        self.id = id
        self.price = price
        self.begintime = begintime
        self.endtime = endtime
        self.ptype = ptype

class Para_DayAheadPlan:
    def __init__(self,day,conn):
        self.day = day
        self.__set_plan(day, conn)

    def __set_plan(self,day,conn):
        sqlstr = ''.join(["select ID,begintime,endtime,Price,BuyorSale,Num"
                         ," from Mod_DayAheadPlan where BeginTime >= '", day
                          , "' and BeginTime < dateadd(dd,1,'", day, "')"])
        self.plandf = pd.read_sql(sqlstr, conn)

    def get_plan(self,begintime,endtime):
        return self.plandf[(begintime <= self.plandf['begintime']) & (endtime >= self.plandf['endtime'])]

class Para_Policy:
    def __init__(self,charge_Or_dis,Num):
        self.charge_Or_dis = charge_Or_dis
        self.Num = Num

class Para_DoLog:

    def __init__(self,begintime,endtime,policy,policyNum,plan='',planNum=0,planprice=0,planbuy=0,buy=0,sale=0
                 ,plansale=0,charge=0,discharge=0,money_use=0,money_useOri=0,storage_KWH=0,price_buy=0,price_sale=0
                 ,real_gene=0,real_load=0):
        self.begintime = begintime
        self.endtime = endtime
        self.policy = policy
        self.policyNum = policyNum
        self.plan = plan
        self.planNum = planNum
        self.planprice = planprice
        self.planbuy = planbuy
        self.plansale = plansale
        self.charge = charge
        self.discharge = discharge
        self.money_use = money_use
        self.money_useOri = money_useOri
        self.storage_KWH = storage_KWH
        self.price_buy = price_buy
        self.price_sale = price_sale
        self.real_gene = real_gene
        self.real_load = real_load
        self.buy = buy
        self.sale = sale

    def tolist(self):
        return [self.begintime,self.endtime,self.policy,self.policyNum,self.plan,self.planNum,self.planprice
            ,self.planbuy,self.plansale,self.buy,self.sale,self.charge,self.discharge,self.money_use,self.money_useOri,self.storage_KWH
            ,self.price_buy,self.price_sale,self.real_gene,self.real_load]

    @staticmethod
    def get_names():
        return ['begintime','endtime','policy','policyNum','plan','planNum','planprice'
            ,'planbuy','plansale','buy','sale','charge','discharge','money_use','money_useOri','storage_KWH'
            ,'price_buy','price_sale','real_gene','real_load']

# 用于在循环中保存是否时间中已经使用过的用电和已经满足的负载
class Para_UseLog:
    def __init__(self):
        self.df = pd.DataFrame([],columns=['begintime','endtime','UseGene','DoLoad'])

    def get_LogRow(self,begintime,endtime):
        df = self.df[(self.df['begintime'] == begintime) & (self.df['endtime']==endtime)]
        if len(df) == 0:
            return None
        else:
            return df.iloc[0,:]

    def Add_Row(self,begintime,endtime,UseGene,DoLoad):
        self.df = self.df.append(pd.DataFrame([[begintime,endtime,UseGene,DoLoad]]
                                    ,columns=['begintime','endtime','UseGene','DoLoad']))

if __name__ == '__main__':

    print('')
