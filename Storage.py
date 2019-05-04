# -*- coding: utf-8 -*-

# @File       : Storage.py
# @Date       : 2019-04-26
# @Author     : Jerold
# @Description:

import pandas as pd
from datetime import *
from Algorithm import *

class Storage:
    """
        Storage Model
    """
    def __init__(self,name,conn):
        self.MaxKWH = 1000
        self.MaxChargeSpeed = 200
        self.MaxDisChargeSpeed = 200
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