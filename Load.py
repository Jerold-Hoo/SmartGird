# -*- coding: utf-8 -*-

# @File       : Load.py
# @Date       : 2019-04-26
# @Author     : Jerold
# @Description:

import pandas as pd
from datetime import *
from Algorithm import *

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
    def get_need_KWH(self, pp_higher, generator, storage, dayplan):
        need_perserve = 0 # 循环中当前阶段累计可以存储的电力
        need_perserve_max = 0  # 此段中需要的存储空间的最大值
        nedd_KWH = 0
        begintime = pp_higher.begintime
        endtime = pp_higher.begintime

        loaddf = self.get_predict_DF(begintime,endtime)
        genedf = generator.get_predict_DF(begintime,endtime)
        plan = dayplan.get_plan(begintime, endtime)
        newDF = pd.merge(loaddf, genedf,on=('begintime','endtime'),how='inner',suffixes=('_l', '_g'))
        newDF = pd.merge(newDF, plan,on=('begintime', 'endtime'), how='inner')

        for idx,row in newDF.iterrows():
            # 1 如果是卖电的计划： 发电 + 电池需要预留 - 负载 = 卖电额：
            if row['BuyorSale'] == 'Sale':
                #  卖电额 -（发电 - 负载）  = 电池需要预留
                this_need = min(storage.MaxKWH, storage.get_disC_speedlimit() / 3600 * (row['endtime'] - row['begintime']).seconds
                            ,row['Num'] - (row['SUMKWH_g'] - row['SUMKWH_l']))

            # 2 如果是买电的计划： 负载 -（发电 + 买电额）= 可充电量：
            if row['BuyorSale'] == 'Buy':
                can_charge = min(storage.MaxKWH,
                                storage.get_C_speedlimit() / 3600 * (row['endtime'] - row['begintime']).seconds
                                , row['SUMKWH_l']) - (row['SUMKWH_g'] + row['Num'])
                # 需求等于可充电量的负值
                this_need = -1 * can_charge

            # 如果有多的，即不需要预留，反而可充，需求为0
            if this_need < 0:
                can_charge = need_perserve * -1
                this_need = 0
                need_perserve += can_charge
                # 如果直接能将电池充满则充满
                if need_perserve >= storage.MaxKWH:
                    need_perserve = storage.MaxKWH
                    need_perserve_max = storage.MaxKWH
                else:
                    if need_perserve > need_perserve_max:
                        need_perserve_max = need_perserve

            # 增加此段的需求
            nedd_KWH += this_need
            # 超出电池上限，则直接设置为上限
            if nedd_KWH >= storage.MaxKWH:
                nedd_KWH = storage.MaxKWH
                break
        """
        for idx,row in newDF.iterrows():
                # 负载比发电多，需要电池支援
                if row['SUMKWH_l'] >= row['SUMKWH_g']:
                    # 需要的电池电力为 限速下最多可放电、电池最大值、需求缺口 中的较小值
                    this_need = min(storage.MaxKWH, storage.get_C_speedlimit() / 3600 * (row['endtime'] - row['begintime']).seconds, row['SUMKWH_l'] - row['SUMKWH_g'])

                    # 如果此段的需求可以被此段存储的覆盖，则已存储全部使用掉
                    if this_need >= need_perserve:
                        this_need = this_need - need_perserve
                        need_perserve = 0
                    # 否则用已存储来优先覆盖需求
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
        """

        # 如果需要预留的空间 和 需要预留的电量之和 大于电池上限,则优先预留电池空间，避免浪费发电
        if need_perserve_max + nedd_KWH > storage.MaxKWH:
            nedd_KWH = storage.MaxKWH - need_perserve_max

        return nedd_KWH