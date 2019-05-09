# -*- coding: utf-8 -*-

# @File       : Generator.py
# @Date       : 2019-04-26
# @Author     : Jerold
# @Description:

import pandas as pd
from datetime import *
from Algorithm import *
import random

class Generator:
    """
        Storage Model
    """
    def __init__(self, name, day, conn, frash_ratio=0.2):

        self.name = name
        self.__set_predict_gene(day, conn)
        self.__set_real_gene(day, conn)
        self.stdDF = self.real_DF.copy()
        self.day = day
        self.conn = conn
        self.frash_ratio = frash_ratio

    def __set_predict_gene(self, day, conn):
        sqlstr = ''.join(["select [begintime],[endtime],[beginvalue],[endvalue],[SUMKWH] from Mod_GeneratoerPredict_S where Name = '"
                             , self.name, "' and begintime >= '", day, "' and begintime < dateadd(dd,1,'", day, "') order by begintime"])
        self.predict_DF = pd.read_sql(sqlstr, conn)

    def __set_real_gene(self, day, conn):
        sqlstr = ''.join(["select [begintime],[endtime],[beginvalue],[endvalue],[SUMKWH] from Mod_Generator_S where Name = '"
                             , self.name, "' and begintime >= '", day, "' and begintime < dateadd(dd,1,'", day, "') order by begintime"])
        self.real_DF = pd.read_sql(sqlstr, conn)

    def refrash(self,timenow,IsRandom=True):
        # 随机变化
        if IsRandom:
            for idx in self.real_DF.index:
                if self.real_DF.loc[idx, 'endtime'] >= timenow:
                    numpre = random.uniform(self.frash_ratio * -1,self.frash_ratio)
                    numreal = random.uniform(self.frash_ratio * -1, self.frash_ratio)
                    self.predict_DF.loc[idx,'SUMKWH'] = (1 + numpre) * self.stdDF.loc[idx,'SUMKWH']
                    self.real_DF.loc[idx, 'SUMKWH'] = (1 + numreal) * self.stdDF.loc[idx,'SUMKWH']
        else:
            self.__set_predict_gene(self.day,self.conn)
            self.__set_real_gene(self.day, self.conn)

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