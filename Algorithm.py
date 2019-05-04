# -*- coding: utf-8 -*-

# @File       : Algorithm.py
# @Date       : 2019-04-26
# @Author     : Jerold
# @Description:

import pandas as pd
from datetime import *

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
