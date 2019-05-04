# -*- coding: utf-8 -*-

# @File       : ModelAlgo.py
# @Date       : 2019-04-26
# @Author     : Jerold
# @Description:

import pandas as pd
from datetime import *
from Load import *
from Storage import *
from Generator import *
from Para import *

# 动态操作算法
def gird_model_dynamic(timenow,storage,load,generator,pricetable,dayplan):
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
        charge_target = load.get_need_KWH(pp_higher, generator, storage, dayplan) / storage.get_disC_rate()
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
                    if (pp_lower.price / storage.get_disC_rate() / storage.get_C_rate() < pp_higher.price
                        and (pp_lower.id not in can_charged_dict.keys() or can_charged_dict[pp_lower.id] != 0) ):

                        thisplan = dayplan.get_plan()

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

def Do_policy(timenow, timeD , policy, buypriceNow, salepriceNow, storage, load, generator, dayplan):
    loadrow = load.get_real_DF(timenow, timenow + timeD).loc[0,:]
    generow = generator.get_real_DF(timenow, timenow + timeD).loc[0,:]
    money_useOri = max(0,buypriceNow *(loadrow['SUMKWH']-generow['SUMKWH']))
    plans = dayplan.get_plan(timenow, timenow + timeD)
    plan = plans.loc[plans.index[0],:]
    BuyorSale = plan['BuyorSale']
    planNum = plan['Num']

    #（计划买电 + 发电) = 电来源 可以放电 = 额外电来源
    # (计划卖电 + 负载) = 电用处 需要充电 = 额外电用处
    # 电来源 - 电用处 = 需要再买 负数则为需要再卖。

    if plan['BuyorSale'] == 'Buy':
        planbuy = plan['Num']
        plansale = 0
    else:
        planbuy = 0
        plansale = plan['Num']

    if policy.charge_Or_dis == 'charge':
        need_charge = policy.Num
        can_useSt = 0
    elif policy.charge_Or_dis == 'discharge':
        need_charge = 0
        can_useSt = policy.Num
    else:
        need_charge = 0
        can_useSt = 0

    need_charge = min(need_charge,storage.get_C_speedlimit() / 3600 * timeD.seconds, storage.MaxKWH - storage.get_SOCKWh())
    can_useSt = min(can_useSt,storage.get_disC_speedlimit() / 3600 * timeD.seconds, storage.get_SOCKWh())

    ecc_in = planbuy + generow['SUMKWH'] + can_useSt * storage.get_disC_rate()
    ecc_out = plansale + loadrow['SUMKWH'] + need_charge/storage.get_C_rate()

    # 输出大于输入 需要再买
    if ecc_out >= ecc_in:
        buy = ecc_out - ecc_in
        sale = 0
    # 否则需要再卖
    else:
        sale = ecc_in - ecc_out
        buy = 0

    money_use = planbuy * plan['Price'] + buy * buypriceNow - plansale * plan['Price'] - sale * salepriceNow
    storage.charge(need_charge)
    storage.discharge(can_useSt)
    Do_log = Para_DoLog(timenow, timenow + timeD, policy.charge_Or_dis, policy.Num, plan=plan['BuyorSale'], planNum=planNum,planprice=plan['Price']
                        , planbuy=planbuy, plansale=plansale, charge=need_charge,discharge=can_useSt
                        , money_use=money_use, money_useOri=money_useOri
                        , storage_KWH=storage.get_SOCKWh(), price_buy=buypriceNow, price_sale=salepriceNow
                        , real_gene=generow['SUMKWH'], real_load=loadrow['SUMKWH'])

    """
    # 正常策略 不充不放
    if policy.charge_Or_dis == 'none':
        # 如果本段计划是购买 计划购电相当于发电
        if plan['BuyorSale'] == 'Buy':
            # 如果实际负载大于实际发电（计划购买）:正常负载、正常买电
            if loadrow['SUMKWH']>= generow['SUMKWH'] + planNum:
                planbuy = planNum
                buy = loadrow['SUMKWH'] - generow['SUMKWH'] - planNum
                charge = 0
                sale = 0
            # 实际发电大于实际负载:回充多余的
            else:
                planbuy = planNum
                buy = 0
                can_charge = generow['SUMKWH'] + planNum - loadrow['SUMKWH']
                if can_charge > min(storage.get_C_speedlimit() / 3600 * timeD.seconds,storage.MaxKWH - storage.get_SOCKWh()):
                    charge = min(storage.get_C_speedlimit() / 3600 * timeD.seconds,storage.MaxKWH - storage.get_SOCKWh())
                    sale = can_charge - charge
                else:
                    charge = can_charge
                    sale = 0

        # 如果本段计划是卖电 卖电相当于负载
        if plan['BuyorSale'] == 'Sale':

            # 如果实际负载（计划卖电）大于实际发电:买电支撑
            if loadrow['SUMKWH'] + planNum >= generow['SUMKWH']:
                plansale = planNum
                buy = loadrow['SUMKWH'] + planNum - generow['SUMKWH']
                charge = 0
                sale = 0
            # 实际发电大于实际负载:回充多余的
            else:
                planbuy = planNum
                buy = 0
                can_charge = generow['SUMKWH'] - planNum - loadrow['SUMKWH']
                if can_charge > min(storage.get_C_speedlimit() / 3600 * timeD.seconds,
                                    storage.MaxKWH - storage.get_SOCKWh()):
                    charge = min(storage.get_C_speedlimit() / 3600 * timeD.seconds,
                                 storage.MaxKWH - storage.get_SOCKWh())
                    sale = can_charge - charge
                else:
                    charge = can_charge
                    sale = 0

        money_use = planbuy * plan['Price'] + buy * buypriceNow -
        storage.charge(charge)
        Do_log = Para_DoLog(timenow,timenow + timeD,policy.charge_Or_dis,0,plan=plan['BuyorSale'],planNum=planNum,planprice=plan['Price']
                            ,planbuy=planbuy,charge=charge,money_use=money_use,price_buyOri=price_buyOri
                            ,storage_KWH=storage.get_SOCKWh(),price_buy=buypriceNow,price_sale=salepriceNow
                            ,real_gene=generow['SUMKWH'],real_load=loadrow['SUMKWH'])

    # 策略需要充电
    if policy.charge_Or_dis == 'charge':
        # 本时段总充电数额：  需要充电、可以充电其中的较小值
        charge_All = min(storage.get_C_speedlimit() / 3600 * timeD.seconds,policy.Num,storage.MaxKWH-storage.get_SOCKWh())

        # 计划是买电 (相当于发电)
        if plan['BuyorSale'] == 'Buy':

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
    """

    return Do_log

def gird_model(name,day,timeDs,engine):
    #pd.set_option('display.max_columns', None)
    #pd.set_option('display.max_rows', None)
    res = []
    generator = Generator(name, day, engine)
    load = Load(name, day, engine)
    storage = Storage(name, engine)
    pricetable = Para_PriceTable(day, engine)
    dayplan = Para_DayAheadPlan(day,engine)
    timenow = datetime(int(day[0:4]), int(day[5:7]), int(day[8:10]))
    timeD = timedelta(minutes=timeDs)

    while timenow < datetime(int(day[0:4]), int(day[5:7]), int(day[8:10])) + timedelta(days=1):
        print('Do:', timenow,'-------------------------------------------')
        policy = gird_model_dynamic(timenow, storage, load, generator, pricetable, dayplan)
        log = Do_policy(timenow, timeD, policy, pricetable.get_PP_bytime_buy(timenow).price, pricetable.get_PP_bytime_sale(timenow).price, storage, load, generator, dayplan)
        res.append(log.tolist())
        timenow += timeD

    return pd.DataFrame(res,columns=Para_DoLog.get_names())
