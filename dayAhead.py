# -*- coding: utf-8 -*-
"""
Created on Wed May 23 15:52:38 2018

@author: Jerold
"""

import pandas as pd
import sqlalchemy as sql
from datetime import *
import matplotlib.pyplot as plt

class Algorithms:
    @staticmethod
    def get_SUM_byTime(DF, begintime, endtime ,limitspeed=None):
        SUMKWH = 0
        newDF = Algorithms.get_subDF_byTime(DF, begintime, endtime)
        if limitspeed==None:
            return newDF['SUMKWH'].sum()

        for idx, row in newDF:
            speedli_MaxSum = (row['endtime']-row['begintime']).seconds * limitspeed / 3600.00
            SUMKWH += min(speedli_MaxSum,row['SUMKWH'])

        return SUMKWH

    @staticmethod
    def get_subDF_byTime(DF, begintime, endtime):
        findbegin = 0
        DFnew = pd.DataFrame([],columns=DF.columns)

        for idx, row in DF.iterrows():
            periodBe = row['begintime']
            periodEnd = row['endtime']
            periodSum = row['SUMKWH']
            periodBeV = row['beginvalue']
            periodEndV = row['endvalue']

            # 开始时间段还没到时
            if findbegin == 0:
                # 找到开始时间
                if begintime >= periodBe and begintime < periodEnd:
                    findbegin = 1
                    # 开始时间段中的总数据 需要判断停止时间是否也在本时间段中：
                    # 如果在，则直接计算本时间段
                    if endtime > periodBe and endtime <= periodEnd:
                        newSum = Algorithms.get_midSum_fromP(periodBe, periodEnd, periodSum, begintime, endtime)
                        newBeValue = Algorithms.get_midvalue_fromP(periodBe, periodEnd, periodBeV, periodEndV, begintime)
                        newEndValue = Algorithms.get_midvalue_fromP(periodBe, periodEnd, periodBeV, periodEndV, endtime)
                        DFnew = DFnew.append(pd.DataFrame([[begintime,endtime,newBeValue,newEndValue,newSum]],columns=DF.columns),ignore_index=True)
                        break
                    # 如果不在
                    else:
                        newSum = Algorithms.get_midSum_fromP(periodBe, periodEnd, periodSum, begintime, periodEnd)
                        newBeValue = Algorithms.get_midvalue_fromP(periodBe, periodEnd, periodBeV, periodEndV, begintime)
                        DFnew = DFnew.append(pd.DataFrame([[begintime,periodEnd,newBeValue,periodEndV,newSum]],columns=DF.columns),ignore_index=True)
                    continue

            # 开始时间段过了
            if findbegin == 1:
                # 如果结果是在此段，则直接计算本时间段
                if endtime > periodBe and endtime <= periodEnd:
                    newSum = Algorithms.get_midSum_fromP(periodBe, periodEnd, periodSum, periodBe, endtime)
                    newEndValue = Algorithms.get_midvalue_fromP(periodBe, periodEnd, periodBeV, periodEndV, endtime)
                    DFnew = DFnew.append(pd.DataFrame([[periodBe,endtime,periodBeV,newEndValue,newSum]],columns=DF.columns),ignore_index=True)
                    break
                # 如果不在
                else:
                    DFnew = DFnew.append(row,ignore_index=True)

        return DFnew

    @staticmethod
    def get_midvalue_fromP(begintime, endtime, beginvalue, endvalue ,targettime):
        return endvalue - (endvalue - beginvalue) / (endtime - begintime).seconds * (endtime - targettime).seconds

    @staticmethod
    def get_midSum_fromP(begintime, endtime, SUMKWH, targetBe ,targetEnd):
        return (targetEnd - targetBe).seconds / (endtime - begintime).seconds * SUMKWH

class Storage:
    """
        Storage Model
    """
    def __init__(self,name,conn):
        self.MaxKWH = 600
        self.MaxChargeSpeed = 100
        self.MaxDisChargeSpeed = 100
        self.now_KWH = 0
        self.charge_rate = 0.95
        self.discharge_rate = 0.95

    def get_disC_speedlimit(self):
        return self.MaxChargeSpeed

    def get_C_speedlimit(self):
        return self.MaxDisChargeSpeed

    def get_C_rate(self,speed = 0):
        return self.charge_rate

    def get_disC_rate(self,speed = 0):
        return self.discharge_rate

    # need todo 获取当时的实际存量
    def get_SOCKWh(self):
        return self.now_KWH

    def charge(self, chargeNum, chaegespeed = 0):
        self.now_KWH += chargeNum
        if self.now_KWH >= self.MaxKWH:
            self.now_KWH = self.MaxKWH
        return

    def discharge(self, dischargeNum, chaegespeed = 0):
        self.now_KWH -= dischargeNum
        if self.now_KWH < 0:
            self.now_KWH = 0
        return

    # 在一段时间内可以放电的最大值  有问题，待处理
    def can_discharge(self, begintime, endtime, generator, load):
        can_dischargeKWH = 0
        can_charge_forme = 0
        loaddf = generator.get_predict_DF(begintime, endtime)
        genedf = load.get_predict_DF(begintime, endtime)
        newDF = pd.merge(loaddf, genedf, on=('begintime', 'endtime'), how='inner', suffixes=('_l', '_g'))

        for idx, row in newDF.iterrows():
            # 负载更大，需要放电
            if row['SUNKWH_l'] >= row['SUNKWH_g']:
                this_can_disC = min((row['SUNKWH_l'] - row['SUNKWH_g']),self.MaxDisChargeSpeed/3600*(row['endtime']-row['begintime']))
                can_dischargeKWH += this_can_disC / self.get_disC_rate()
                if can_dischargeKWH >= self.MaxKWH:
                    return self.MaxKWH
            else:
                this_can_C = (row['SUNKWH_g'] - row['SUNKWH_l']) * self.get_C_rate()
                can_charge_forme += this_can_C

        return can_dischargeKWH

    # 在一段时间内可以充进电量的最大值
    def can_charge(self, begintime, endtime):
        return min(self.MaxKWH,self.MaxChargeSpeed / 3600 * (endtime-begintime).seconds)
        """
        #can_chargeKWH = 0
        #df_predict = generator.get_predict_DF(begintime, endtime)
        for idx,row in df_predict.iterrows():
            this_charge = min(row['SUMKWH'] * self.get_C_rate(),self.MaxChargeSpeed / 3600 * (row['endtime']-row['begintime']).seconds)
            can_chargeKWH += this_charge
            if can_chargeKWH >= self.MaxKWH:
                return self.MaxKWH

        return can_chargeKWH
        """

class Generator:
    """
        Storage Model
    """
    def __init__(self, name, day, conn):

        self.name = name
        self.__set_predict_gene(day, conn)
        self.__set_real_gene(day, conn)

    def __set_predict_gene(self, day, conn):
        sqlstr = ''.join(["select [begintime],[endtime],[beginvalue],[endvalue],[SUMKWH] from Mod_GeneratoerPredict_S where Name = '"
                             , self.name, "' and begintime >= '", day, "' and begintime < dateadd(dd,1,'", day, "') order by begintime"])
        self.predict_DF = pd.read_sql(sqlstr, conn)

    def __set_real_gene(self, day, conn):
        sqlstr = ''.join(["select [begintime],[endtime],[beginvalue],[endvalue],[SUMKWH] from Mod_Generator_S where Name = '"
                             , self.name, "' and begintime >= '", day, "' and begintime < dateadd(dd,1,'", day, "') order by begintime"])
        self.real_DF = pd.read_sql(sqlstr, conn)

    def get_predict_DF(self,begintime,endtime):
        return Algorithms.get_subDF_byTime(self.predict_DF, begintime, endtime)

    def get_real_DF(self, begintime, endtime):
        return Algorithms.get_subDF_byTime(self.real_DF, begintime, endtime)

    # 获取一段时间内的预测发电量
    def get_predict_KWHSUM(self,begintime,endtime,limitspeed=None):
        return Algorithms.get_SUM_byTime(self.predict_DF, begintime, endtime, limitspeed)

    # 获取一段时间内的真实发电量
    def get_real_KWHSUM(self,begintime,endtime,limitspeed=None):
        return Algorithms.get_SUM_byTime(self.real_DF, begintime, endtime, limitspeed)

class Load:
    """
        Load Model
    """

    def __init__(self, name, day, conn):
        self.name = name
        self.__set_predict_load(day, conn)
        self.__set_real_load(day, conn)

    def __set_predict_load(self, day, conn):
        sqlstr = ''.join(["select [begintime],[endtime],[beginvalue],[endvalue],[SUMKWH] from Mod_LoadPredict_S where Name = '"
                             , self.name, "' and begintime >= '", day, "' and begintime < dateadd(dd,1,'", day, "') order by begintime"])
        self.predict_DF = pd.read_sql(sqlstr, conn)

    def __set_real_load(self, day, conn):
        sqlstr = ''.join(["select [begintime],[endtime],[beginvalue],[endvalue],[SUMKWH] from Mod_Load_S where Name = '"
                             , self.name, "' and begintime >= '", day, "' and begintime < dateadd(dd,1,'", day, "') order by begintime"])
        self.real_DF = pd.read_sql(sqlstr, conn)

    def get_predict_DF(self,begintime,endtime):
        return Algorithms.get_subDF_byTime(self.predict_DF, begintime, endtime)

    def get_real_DF(self, begintime, endtime):
        return Algorithms.get_subDF_byTime(self.real_DF, begintime, endtime)

    # 获取一段时间内的预测用电量
    def get_predict_KWHSUM(self, begintime, endtime, limitspeed=None):
        return Algorithms.get_SUM_byTime(self.predict_DF, begintime, endtime, limitspeed)

    # 获取一段时间内的实际用电量
    def get_real_KWHSUM(self, begintime, endtime ,limitspeed=None):
        return Algorithms.get_SUM_byTime(self.real_DF, begintime, endtime, limitspeed)

    # 一段时间内为了尽量满足负载所需要预先存储的电量
    def get_need_KWH(self, begintime, endtime, generator, storage):
        need_perserve = 0
        need_perserve_max = 0
        nedd_KWH = 0

        loaddf = self.get_predict_DF(begintime,endtime)
        genedf = generator.get_predict_DF(begintime,endtime)
        newDF = pd.merge(loaddf,genedf,on=('begintime','endtime'),how='inner',suffixes=('_l', '_g'))

        for idx,row in newDF.iterrows():

            # 负载比发电多，需要电池支援
            if row['SUMKWH_l'] >= row['SUMKWH_g']:
                this_need = min(storage.MaxKWH, storage.get_C_speedlimit() / 3600 * (row['endtime'] - row['begintime']).seconds, row['SUMKWH_l'] - row['SUMKWH_g'])

                # 如果此段的需求可以被此段存储的覆盖，则存储全部使用掉
                if this_need >= need_perserve:
                    this_need = this_need - need_perserve
                    need_perserve = 0
                else:
                    this_need = need_perserve - this_need
                    this_need = 0

                # 增加此段的需求
                nedd_KWH += this_need
                # 超出电池上限，则直接设置为上限
                if nedd_KWH >= storage.MaxKWH:
                    nedd_KWH = storage.MaxKWH
                    break

            # 发电比负载多可以回充支持
            else:
                can_charge = min(storage.MaxKWH, storage.get_disC_speedlimit() / 3600 * (row['endtime']-row['begintime']).seconds, row['SUMKWH_g'] - row['SUMKWH_l'])
                need_perserve += can_charge
                # 如果直接能将电池充满则充满
                if need_perserve >= storage.MaxKWH:
                    need_perserve = storage.MaxKWH
                    need_perserve_max = storage.MaxKWH
                else:
                    if need_perserve > need_perserve_max:
                        need_perserve_max = need_perserve

        # 如果需要预留的空间 和 需要预留的电量之和 大于电池上限,则优先预留电池空间，避免浪费发电
        if need_perserve_max + nedd_KWH > storage.MaxKWH:
            nedd_KWH = storage.MaxKWH - need_perserve_max

        return nedd_KWH

class Para_PriceTable:

    def __init__(self,day,conn):
        self.pp_dictbuy = {}
        self.day = day
        self.__set_buyprice(day, conn)
        self.__set_saleprice(day, conn)

    def __get_buypp_by_idx(self,idx,row):
        if self.pp_dictbuy.get(idx) is None:
            priceperiod = Para_Priceperiod(row['ID'],row['BuyPrice'],row['BeginTime'],row['EndTime'])
            self.pp_dictbuy[idx] = priceperiod
        else:
            priceperiod = self.pp_dictbuy[idx]
        return priceperiod


    def __set_buyprice(self, day, conn):
        sqlstr = ''.join(["select ID,BeginTime,EndTime,BuyPrice from Mod_PriceTableBuy where BeginTime >= '", day
                          , "' and BeginTime < dateadd(dd,1,'", day, "')"])
        self.buy_table = pd.read_sql(sqlstr, conn)

    def __set_saleprice(self, day, conn):
        sqlstr = ''.join(["select ID,BeginTime,EndTime,SalePrice from Mod_PriceTableSale where BeginTime >= '", day
                          , "' and EndTime < dateadd(dd,1,'", day, "')"])
        self.sale_table = pd.read_sql(sqlstr, conn)

    # 根据时间获取买价表时期priceperiod
    def get_PP_bytime_buy(self, time):
        for idx,row in self.buy_table.iterrows():
            if time >= row['BeginTime'] and time < row['EndTime']:
                return self.__get_buypp_by_idx(idx,row)

    # 找出当前时间之后、设置时间之前比自己价格高的idx list
    def get_Idxs_buyhigher_aft(self,timenow,timeend=None,orderby='price desc'):
        res_list = []
        if timeend is None:
            timeend = timenow + timedelta(days=1)
        ppNow = self.get_PP_bytime_buy(timenow)
        for idx,row in self.buy_table.iterrows():
            # 在时间范围内，且价格比自己高的
            if row['BeginTime'] > timenow and row['BeginTime'] < timeend and row['BuyPrice'] > ppNow.price:
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
            if row['BeginTime'] > timenow and row['BeginTime'] < timeend and row['BuyPrice'] <= ppNow.price:
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

    def __init__(self, id, price, begintime, endtime):
        self.id = id
        self.price = price
        self.begintime = begintime
        self.endtime = endtime
        #self.orderidx = orderidx

class Para_Policy:
    def __init__(self,charge_Or_dis,Num):
        self.charge_Or_dis = charge_Or_dis
        self.Num = Num

class Para_DoLog:
    def __init__(self,begintime,endtime,policy,policyNum,charge_from_gene=0,discharge_to_load=0,buy_to_load=0,charge_from_buy=0
                 ,price_buy=0,gene_to_load=0,price_buyOri=0,storage_KWH=0,price_buy_std=0,real_gene=0,real_load=0):
        self.begintime = begintime
        self.endtime = endtime
        self.policy = policy
        self.policyNum = policyNum
        self.charge_from_gene = charge_from_gene
        self.discharge_to_load = discharge_to_load
        self.buy_to_load = buy_to_load
        self.price_buy = price_buy
        self.gene_to_load = gene_to_load
        self.price_buyOri = price_buyOri
        self.storage_KWH = storage_KWH
        self.price_buy_std = price_buy_std
        self.real_gene = real_gene
        self.real_load = real_load
        self.charge_from_buy = charge_from_buy

    def tolist(self):
        return [self.begintime,self.endtime,self.policy,self.policyNum,self.charge_from_gene,self.charge_from_buy,self.discharge_to_load
            ,self.buy_to_load,self.gene_to_load,self.price_buy,self.price_buyOri,self.storage_KWH,self.price_buy_std
                ,self.real_gene,self.real_load]

    @staticmethod
    def get_names():
        return ['begintime','endtime','policy','policyNum','charge_from_gene','charge_from_buy','discharge_to_load'
            ,'buy_to_load','gene_to_load','price_buy','price_buyOri','storage_KWH','price_buy_std'
                ,'real_gene','real_load']

# 动态操作算法
def gird_model_dynamic(timenow,storage,load,generator,pricetable):
    pp_now = pricetable.get_PP_bytime_buy(timenow)
    KWH_now = storage.get_SOCKWh() # 现在还有多少电量，优先为后面留着
    KWH_can_use_fromST = KWH_now # 现在还可以使用的

    now_can_charge = storage.can_charge(timenow,pp_now.endtime) # 当前时段总共还能冲多少

    need_now_to_charge_SUM = 0  # 需要现在去充多少电

    can_charged_dict = {} # 一个字典维护不同时段还可以继续充电的余量。

    # 先判断为后续更高价位阶段区间是否需要留电

    # 循环找到比自己更高价位的区间进行判断他们是否需要
    for pp_higher in pricetable.get_Idxs_buyhigher_aft(timenow=timenow,timeend=None,orderby='price desc'):

        #period_load_predict = load.get_predict_KWHSUM(begintime=pp_higher.begintime, endtime=pp_higher.endtime) # 时间段内需求量
        #period_gene_predict = generator.get_predict_KWHSUM(begintime=pp_higher.begintime, endtime=pp_higher.endtime) # 时段内发电量

        # 计算此时段是否需要在初始时刻留电 除以放电比例
        charge_target = load.get_need_KWH(pp_higher.begintime, pp_higher.endtime, generator, storage) / storage.get_disC_rate()
        # 如果此更高价时段有用电缺口
        if charge_target > 0:

            # 电池需求已经满足 则把这部分全部预留给此高价时段，当前时段不需要充电预留，并继续循环看下一高价时段
            if charge_target < KWH_can_use_fromST:
                KWH_can_use_fromST -= charge_target
            #电池需求没有满足,则判断可否让其它时段帮助充电，此时充电目标为时段需求减去电池固有（电池固有全部给此时段）
            else:
                charge_target = charge_target - KWH_can_use_fromST
                KWH_can_use_fromST = 0

                periodslower_have_charged = 0  # 后续低电价区已经为此高电价区充电的数量
                need_now_to_charge = True # 需要当前时段充电吗

                # 需要判断后续比本时段价格低的发电来充电能是否满足需求:
                for pp_lower in pricetable.get_Idxs_buylower_aft(timenow=timenow, timeend=pp_higher.begintime, orderby='price desc'):

                    # 如果该时段的发电计算溢价之后依然划算，且没有充过或者还有可充的预期电量,则计算本时段可以为该时段充多少电
                    if ( pp_lower.price / storage.get_disC_rate() / storage.get_C_rate() < pp_higher.price
                        and (pp_lower.id not in can_charged_dict.keys() or can_charged_dict[pp_lower.id] != 0) ):

                        # 如果该时段没有充过,则预期可充电量为总预期可充电量
                        if pp_lower.id not in can_charged_dict.keys():
                            periodlower_cancharge_predict = storage.can_charge(pp_lower.begintime,pp_higher.endtime) #可充电的预期
                        # 如果充过但是还有余额则为余额
                        elif can_charged_dict[pp_lower.id] != 0:
                            periodlower_cancharge_predict = can_charged_dict[pp_lower.id]

                        # 如果此低价时段还充不够目标，则将本时段全部充电并且循环查找下一低价时段
                        if periodslower_have_charged + periodlower_cancharge_predict < charge_target:
                            can_charged_dict[pp_lower.id] = 0 # -1 标记该时段的电全部充完了
                            periodslower_have_charged += periodlower_cancharge_predict
                        # 如果能充够数目了，则充入缺口部分,并且不再继续寻找此段了
                        else:
                            can_charged_dict[pp_lower.id] = periodlower_cancharge_predict - (charge_target - periodslower_have_charged)
                            need_now_to_charge = False
                            break

                need_charge_KWH = 0 # 计算需要为此高时段充多少电
                # 如果需要本时段去充电，计算充多少
                if need_now_to_charge:
                    need_charge_KWH = charge_target - periodslower_have_charged
                    # 如果需要本时段充电的+本时段已经充电的超出上限了则本时段充电到上限，并不再计算是否为其它高阶段充电
                    if need_now_to_charge_SUM + need_charge_KWH >= now_can_charge:
                        need_now_to_charge_SUM = now_can_charge
                        break;  # 已经充满则跳出寻找更高价段的循环。

                    else: # 如果没有超出，则需要充多少充多少
                        need_now_to_charge_SUM += need_charge_KWH

    # 如果需要充电，则使用充电策略冲入 Num的电量
    if need_now_to_charge_SUM > 0:
        policy = Para_Policy('charge',need_now_to_charge_SUM)

    # 如果则不需要充电，判断自己能用多少电
    elif need_now_to_charge_SUM == 0 and KWH_can_use_fromST > 0:
        # 用电缺口大于可用电则可放不超出KWH_can_use_fromST的电量
        policy = Para_Policy('discharge', KWH_can_use_fromST)
    # 不充不放，自用
    else:
        policy = Para_Policy('none',0)

    print('policy:',policy.charge_Or_dis,policy.Num)
    return policy

def Do_policy(timenow, timeD , policy, buypriceNow, storage, load, generator):
    loadrow = load.get_real_DF(timenow, timenow + timeD).loc[0,:]
    generow = generator.get_real_DF(timenow, timenow + timeD).loc[0,:]
    price_buyOri = max(0,buypriceNow *(loadrow['SUMKWH']-generow['SUMKWH']))
    # 正常策略 不充不放
    if policy.charge_Or_dis == 'none':
        # 如果实际负载大于实际发电:正常负载、正常买电
        if loadrow['SUMKWH'] >= generow['SUMKWH']:
            gene_to_load = generow['SUMKWH']
            buy_to_load = loadrow['SUMKWH'] - generow['SUMKWH']
            charge_from_gene = 0
        # 实际发电大于实际负载:回充多余的
        else:
            gene_to_load = loadrow['SUMKWH']
            buy_to_load = 0
            charge_from_gene = min((generow['SUMKWH'] - loadrow['SUMKWH'])*storage.get_C_rate()
                                   , storage.get_C_speedlimit() / 3600 * timeD.seconds
                                   , storage.MaxKWH - storage.get_SOCKWh())

        storage.charge(charge_from_gene)
        Do_log = Para_DoLog(timenow,timenow + timeD,policy.charge_Or_dis,0,charge_from_gene=charge_from_gene
                            ,buy_to_load=buy_to_load,price_buy=buypriceNow * buy_to_load,gene_to_load = gene_to_load
                            ,price_buyOri=price_buyOri,storage_KWH=storage.get_SOCKWh(),price_buy_std=buypriceNow
                            ,real_gene=generow['SUMKWH'],real_load=loadrow['SUMKWH'])

    # 策略需要充电
    if policy.charge_Or_dis == 'charge':

        # 本时段总充电数额：  需要充电、可以充电其中的较小值
        charge_All = min(storage.get_C_speedlimit() / 3600 * timeD.seconds,policy.Num,storage.MaxKWH-storage.get_SOCKWh())

        # 如果发电能满足 则由发电提供
        if generow['SUMKWH'] * storage.get_C_rate() >= charge_All :
            charge_from_gene = charge_All  # 发电提供充电
            charge_from_buy = 0
            gene_can_use = generow['SUMKWH'] - charge_All / storage.get_C_rate() # 剩余可用发电
            # 如果剩余发电不能满足负载
            if gene_can_use <= loadrow['SUMKWH']:
                gene_to_load = gene_can_use  # 剩余发电全给负载
                buy_to_load = loadrow['SUMKWH'] - gene_can_use  # 剩余负载买电完成
            # 如果剩余发电能满足负载，多余部分能充则充,
            else:
                gene_to_load = loadrow['SUMKWH'] # 剩余发电完全满足负载
                # 增加剩余可充部分
                charge_from_gene += (storage.MaxKWH-storage.get_SOCKWh()-charge_from_gene,gene_can_use - loadrow['SUMKWH'])
                # 如果总充电超出限速，则按限速控制
                charge_from_gene = min(storage.get_C_speedlimit() / 3600 * timeD.seconds,charge_from_gene)
                buy_to_load = 0

        #  如果发电不能满足充电需求、发电全输入且还需要外购。
        else:
            charge_from_gene = generow['SUMKWH'] * storage.get_C_rate() # 发电全输入
            charge_from_buy = charge_All - generow['SUMKWH'] * storage.get_C_rate() # 外购电充电
            buy_to_load = loadrow['SUMKWH'] # 负载全由买电负责
            gene_to_load = 0

        storage.charge(charge_from_gene + charge_from_buy)
        Do_log = Para_DoLog(timenow, timenow + timeD,policy.charge_Or_dis,policy.Num,charge_from_gene=charge_from_gene, buy_to_load=buy_to_load
                            ,price_buy=buypriceNow*(buy_to_load + charge_from_buy / storage.get_C_rate() ),charge_from_buy=charge_from_buy
                            , gene_to_load=gene_to_load,price_buyOri=price_buyOri,storage_KWH=storage.get_SOCKWh(),price_buy_std=buypriceNow
                            , real_gene=generow['SUMKWH'], real_load=loadrow['SUMKWH'])

    # 如果可以用电:
    if policy.charge_Or_dis == 'discharge':
        # 如果不需要用电池电、回充
        if generow['SUMKWH'] >= loadrow['SUMKWH']:
            charge_from_gene = min( (generow['SUMKWH'] - loadrow['SUMKWH']) * storage.get_C_rate(),
                                   storage.get_C_speedlimit() / 3600 * timeD.seconds
                                   ,storage.MaxKWH - storage.get_SOCKWh)
            discharge_to_load = 0
            buy_to_load = 0
            gene_to_load = loadrow['SUMKWH']

        # 需要用电池电 发电全供给负载、剩余负载
        else:
            gene_to_load = generow['SUMKWH']  # 发电全供给负载
            charge_from_gene = 0 # 不可能充电
            #需要
            discharge_to_load = min(loadrow['SUMKWH'] - generow['SUMKWH'], policy.Num, storage.get_disC_speedlimit() / 3600 * timeD.seconds)
            buy_to_load = loadrow['SUMKWH'] - generow['SUMKWH'] - discharge_to_load*storage.get_disC_rate()

        storage.charge(charge_from_gene)
        storage.discharge(discharge_to_load)
        Do_log = Para_DoLog(timenow, timenow + timeD,policy.charge_Or_dis,policy.Num, charge_from_gene=charge_from_gene,discharge_to_load=discharge_to_load
                            , buy_to_load=buy_to_load, price_buy=buypriceNow * buy_to_load
                            , gene_to_load=gene_to_load,price_buyOri=price_buyOri,storage_KWH=storage.get_SOCKWh(),price_buy_std=buypriceNow
                            , real_gene=generow['SUMKWH'], real_load=loadrow['SUMKWH'])
    return Do_log

def gird_model(name,day,timeDs,engine):
    #pd.set_option('display.max_columns', None)
    #pd.set_option('display.max_rows', None)
    res = []
    generator = Generator(name, day, engine)
    load = Load(name, day, engine)
    storage = Storage(name, engine)
    pricetable = Para_PriceTable(day, engine)

    timenow = datetime(int(day[0:4]), int(day[5:7]), int(day[8:10]))
    timeD = timedelta(minutes=timeDs)

    while timenow < datetime(int(day[0:4]), int(day[5:7]), int(day[8:10])) + timedelta(days=1):
        print('Do:', timenow,'-------------------------------------------')
        policy = gird_model_dynamic(timenow, storage, load, generator, pricetable)
        log = Do_policy(timenow, timeD, policy, pricetable.get_PP_bytime_buy(timenow).price, storage, load, generator)
        res.append(log.tolist())
        timenow += timeD

    return pd.DataFrame(res,columns=Para_DoLog.get_names())

def plot(name,engine):
    SQLstr = ''.join(["select * from A_resAll where name = '",name,"' order by begintime"])
    df = pd.read_sql(SQLstr,engine)

    ax0 = plt.subplot(412)
    ax1 = plt.subplot(413)
    ax2 = plt.subplot(414)
    ax3 = plt.subplot(411)
    ax0.plot(df['begintime'], df['real_gene'])
    ax0.plot(df['begintime'], df['real_load'])
    ax0.legend(['real_gene', 'real_load'])

    ax1.plot(df['begintime'], df['charge_from_gene'] + df['charge_from_buy'])
    ax1.plot(df['begintime'], df['charge_from_buy'] + df['buy_to_load'])
    ax1.plot(df['begintime'], df['discharge_to_load'])
    #ax1.plot(df['begintime'], df['gene_to_load'])
    ax1.legend(['charge', 'buy','discharge'])

    ax2.plot(df['begintime'], df['price_buy'])
    ax2.plot(df['begintime'], df['price_buyOri'])
    ax2.legend(['All_buy_UseMoney', 'Ori_buy_UseMoney'])

    ax3.plot(df['begintime'], df['price_buy_std'])
    ax3.legend(['price'])
    plt.show()


if __name__ == '__main__':

    engine = sql.create_engine('mssql+pyodbc://sa:2019newpass.7142sql@118.123.7.142:1433/GIRD_EN?driver=SQL+Server')

    res = gird_model('test','2018-10-01',15,engine)
    #res = gird_model('Eng','2018-02-03',30,engine)

    print(res)
    #res.to_sql('A_res',engine,if_exists='replace')

    plot('test:MAX=1000,SPEED=200,INIT=0,CRate=95,DisCRate=95',engine)
    # 'test:MAX=1000,SPEED=200,INIT=0,CRate=95,DisCRate=95'

