import pandas as pd
import numpy as np
import schedule
import os
import paramiko
from datetime import date, timedelta
import time
import logging
from pymongo import MongoClient 
import json
import glob

def CreateLog(fileName, logPath):
    if not os.path.exists(logPath):
        os.mkdir(logPath)
        logging.basicConfig(
            filename= f'./log/{fileName}', 
            filemode= 'w', 
            format= '%(asctime)s - %(message)s', 
            encoding= 'utf-8'
            )
    else:
        logging.basicConfig(
            filename= f'./log/{fileName}', 
            filemode='a', 
            format='%(asctime)s - %(message)s',
            encoding='utf-8'
            ) 
        
def is_float(value):
    try:
        float(value)
        return True
    except:
        return False
    
def split_data(d_string):
    item_name = []
    line = d_string.replace('\n','').replace(' ','')
    if ("<<" in line) and (">>" in line) :
        item_name_s1 = line.replace('>>','').split('<<')
        for item_name_s2 in item_name_s1 :
#            print('item_name_s2 = ', item_name_s2)
            if ("(" in item_name_s2) and (")" in item_name_s2) :
                item_name_s3 = item_name_s2.split('(')
#                print('item_name_s3 = ', item_name_s3)
                if len(item_name_s3)>1 :
                    for item_name_s4 in item_name_s3 :
                        if (")" in item_name_s4) :
                            item_name_s5 = item_name_s4.split(')')
#                            print('item_name_s5 = ', item_name_s5)
                            for item_name_s6 in item_name_s5 :
                                if ("," in item_name_s6) :
                                    item_name_s7 = item_name_s6.split(',')
#                                    print('item_name_s7 = ', item_name_s7)
                                    item_name = item_name + item_name_s7
                                else :
                                    item_name.append(item_name_s6)  
                        else :
                            item_name.append(item_name_s4)
                else :
                    item_name.append(item_name_s3)
            else :
                item_name.append(item_name_s2)
    elif ("(" in line) and (")" in line) :
        item_name_s1 = line.replace(')','').split('(')
#        print('item_name_s1 = ', item_name_s1)
        if len(item_name_s1)>1 :
            for item_name_s2 in item_name_s1 :
                if ("," in item_name_s2) :
                    item_name_s3 = item_name_s2.split(',')
#                    print('item_name_s3 = ', item_name_s3)
                    for item_name_s4 in item_name_s3 :
                        if ("<<" in item_name_s4) :
                            item_name_s5 = item_name_s4.replace('>>','').split('<<')
                            if len(item_name_s5)>1 :
                                item_name = item_name + item_name_s5
                            else :
                                item_name.append(item_name_s5)
                        else:
                            item_name.append(item_name_s4)
                else:
                    item_name.append(item_name_s2)
        else:
            item_name.append(item_name_s1)
    elif ("," in line) :
        item_name_s1 = line.split(',')
#        print('item_name_s1 = ', item_name_s1)
        if len(item_name_s1)>1 :
            for item_name_s2 in item_name_s1 :
                if ("<<" in item_name_s2) :
                    item_name_s3 = item_name_s2.replace('>>','').split('<<')
#                    print('item_name_s3 = ', item_name_s3)
                    if len(item_name_s3)>1 :
                                item_name = item_name + item_name_s3
                    else :
                        item_name.append(item_name_s3)
                else:
                    item_name.append(item_name_s2)
        else:
            item_name.append(item_name_s1)
    else:
        item_name.append(line)
    
    d_item_value=[]
    d_item_judge=[] 
    ck_no=0
    ck_cnt=0
    for d_item in item_name:
        ck_cnt+=1
        if (ck_no==0) and (ck_cnt==len(item_name)) and (is_float(d_item)==True) :
            d_item_value = d_item_value + [float(d_item)]
            d_item_judge = d_item_judge + ['']
            ck_no=1
        elif (ck_no==0) and (is_float(d_item)==True) :
            d_item_value = d_item_value + [float(d_item)]
            ck_no=1
        elif (ck_no==0) and (is_float(d_item)==False) :
            d_item_judge = d_item_judge + [d_item]
            ck_no=2
        elif (ck_no==2) and (is_float(d_item)==True) :
            d_item_value = d_item_value + [float(d_item)]
            ck_no=0
        elif (ck_no==2) and (is_float(d_item)==False) :
            d_item_judge = d_item_judge + [d_item]
            d_item_value = d_item_value + ['','']
            ck_no=0
        elif (ck_no==1) and (is_float(d_item)==True) :
            d_item_value = d_item_value + [float(d_item)]
            d_item_judge = d_item_judge + ['','']
            ck_no=0
        elif (ck_no==1) and (is_float(d_item)==False) :
            d_item_judge = d_item_judge + [d_item]
            ck_no=0
            
    if len(d_item_value)<5  :
        d_item_value = d_item_value + ['']*(5-len(d_item_value))
    if len(d_item_judge)<5  :
        d_item_judge = d_item_judge + ['']*(5-len(d_item_judge))
        
    return d_item_value, d_item_judge

def split_name(d_string):
    item_name = []
    line = d_string.replace('\n','').replace(' ','')
    if ("<<" in line) and (">>" in line) :
        item_name_s1 = line.replace('>>','').split('<<')
        for item_name_s2 in item_name_s1 :
#            print('item_name_s2 = ', item_name_s2)
            if ("(" in item_name_s2) and (")" in item_name_s2) :
                item_name_s3 = item_name_s2.split('(')
#                print('item_name_s3 = ', item_name_s3)
                if len(item_name_s3)>1 :
                    for item_name_s4 in item_name_s3 :
                        if (")" in item_name_s4) :
                            item_name_s5 = item_name_s4.split(')')
#                            print('item_name_s5 = ', item_name_s5)
                            for item_name_s6 in item_name_s5 :
                                if ("," in item_name_s6) :
                                    item_name_s7 = item_name_s6.split(',')
                                    print('item_name_s7 = ', item_name_s7)
                                    item_name = item_name + item_name_s7
                                else :
                                    item_name.append(item_name_s6)  
                        else :
                            item_name.append(item_name_s4)
                else :
                    item_name.append(item_name_s3)
            else :
                item_name.append(item_name_s2)
    elif ("(" in line) and (")" in line) :
        item_name_s1 = line.split('(')
#        print('item_name_s1 = ', item_name_s1)
        if len(item_name_s1)>1 :
            for item_name_s2 in item_name_s1 :
                if (")" in item_name_s2) :
                    item_name_s3 = item_name_s2.split(')')
#                    print('item_name_s3 = ', item_name_s3)
                    for item_name_s4 in item_name_s3 :
                        if ("," in item_name_s4) :
                            item_name_s5 = item_name_s4.split(',')
#                            print('item_name_s5 = ', item_name_s5)
                            if len(item_name_s5)>1 :
                                item_name = item_name + item_name_s5
                            else :
                                item_name.append(item_name_s5)
                        else:
                            item_name.append(item_name_s4)
                else:
                    item_name.append(item_name_s2)
        else:
            item_name.append(item_name_s1)
    elif ("," in line) :
        item_name_s1 = line.split(',')
#        print('item_name_s1 = ', item_name_s1)
        if len(item_name_s1)>1 :
            for item_name_s2 in item_name_s1 :
                if ("<<" in item_name_s2) :
                    item_name_s3 = item_name_s2.replace('>>','').split('<<')
#                    print('item_name_s3 = ', item_name_s3)
                    if len(item_name_s3)>1 :
                                item_name = item_name + item_name_s3
                    else :
                        item_name.append(item_name_s3)
                else:
                    item_name.append(item_name_s2)
        else:
            item_name.append(item_name_s1)
    else:
        item_name.append(line)
        
    if len(item_name)<6  :
        item_name = item_name + ['']*(6-len(item_name))
        
    return item_name

def process_adr(source_path='U:/uLED/SW/0.0.Adr', source_name='0.0', target_path='./', target_name='0.0'):
    #f = open(source_path + source_name + '.Adr', 'r')
    f = open(source_path, 'r')
    lines = f.readlines()
    EQID=''; AUO_CHIP_ID=''; BIN_1=''; BIN_2=''; RESULT_1=''; RESULT_2=''; GRADE=''; JUDGEMENT=''
    d_no = 0
    header_s = 0
    header_e = 999
    step_s = 0
    step_e = 999
    escape_s = 0
    escape_e = 999
    cum_s = 0
    cum_e = 999
    head_lists = []
    item_name = []
    item_value = []
    item_judge = []

    item_name1 = []
    item_value1 = []
    item_judge1 = []

    ck_ck_list = []

    for line in lines:
        d_no += 1
        line = line.replace('\n','')
        #input_data = line.replace(' ','').split('=')
        input_data = line.split('=')
        if input_data[0] == 'PROGRAM_VER':
            PROGRAM_VER = input_data[1]
        elif input_data[0] == 'EQID':
            EQID = input_data[1]
        elif input_data[0] == 'RECIPE_ID':
            RECIPE_ID = input_data[1]
        elif input_data[0] == 'LOT_ID':
            LOT_ID = input_data[1]
        elif input_data[0] == 'GLASS_ID':
            GLASS_ID = input_data[1]
        elif input_data[0] == 'CHIP_ID':
            CHIP_ID = input_data[1]
        elif input_data[0] == 'CHIP_NO':
            CHIP_NO = input_data[1]
        elif input_data[0] == 'PROC_NO':
            PROC_NO = input_data[1]
        elif input_data[0] == 'OP_ID':
            PROC_NO = input_data[1]
        elif input_data[0] == 'AUO_CHIP_ID':
            AUO_CHIP_ID = input_data[1]

        elif input_data[0] == 'LOT_START_DATE':
            LOT_START_DATE = input_data[1]
        elif input_data[0] == 'LOT_START_TIME':
            LOT_START_TIME = input_data[1]
        elif input_data[0] == 'GLASS_START_DATE':
            GLASS_START_DATE = input_data[1]
        elif input_data[0] == 'GLASS_START_TIME':
            GLASS_START_TIME = input_data[1]
        elif input_data[0] == 'START_DATE':
            CHIP_START_DATE = input_data[1]
        elif input_data[0] == 'START_TIME':
            CHIP_START_TIME = input_data[1]
        elif input_data[0] == 'END_DATE':
            CHIP_END_DATE = input_data[1]
        elif input_data[0] == 'END_TIME':
            CHIP_END_TIME = input_data[1]
        elif input_data[0] == 'CHIP_START_DATE':
            CHIP_START_DATE = input_data[1]
        elif input_data[0] == 'CHIP_START_TIME':
            CHIP_START_TIME = input_data[1]
        elif input_data[0] == 'CHIP_END_DATE':
            CHIP_END_DATE = input_data[1]
        elif input_data[0] == 'CHIP_END_TIME':
            CHIP_END_TIME = input_data[1]

        elif input_data[0] == 'BIN':
            BIN = input_data[1]
        elif input_data[0] == '1_BIN':
            BIN_1 = input_data[1]
        elif input_data[0] == '2_BIN':
            BIN_2 = input_data[1]

        elif input_data[0] == 'RESULT':
            RESULT= input_data[1]
        elif input_data[0] == '1_RESULT':
            RESULT_1= input_data[1]
        elif input_data[0] == '2_RESULT':
            RESULT_2= input_data[1]

        elif input_data[0] == 'GRADE':
            GRADE = input_data[1]
        elif input_data[0] == 'JUDGEMENT':
            JUDGEMENT = input_data[1]


        if(len(input_data))>1:
#            print(input_data)
            d_name = split_name(input_data[0])
            d_value, d_judge = split_data(input_data[1])
#            print(len(d_name), len(d_value), len(d_judge), d_name, d_value, d_judge)
            ck_ck_list.append([len(d_name), len(d_value), len(d_judge)])

        if line[:8] == "[HEADER]" : 
#            print("start process ", line[1:-1], " at line : ", d_no)
            head_name = line[1:-1]
            header_s = d_no
            header_e = 0
        elif (header_e==0) and (("==Step" in line) or (len(line)<1) or ((line[:1] == "[") and (line[-1:] == "]"))) :
#            print("end process ", head_name, " at line : ", d_no-1)
            header_s = 0
            header_e = d_no-1
#            print(item_name, item_value, item_judge)
        elif (header_s + header_e > 0) and (header_e==0) : 
            d_item_name=[]
            d_item_value=[]
            d_item_judge=[]
            if len(input_data)>1 :
                d_item_name = d_item_name + [head_name] + [input_data[0]] + ['']*5
                d_item_value = d_item_value + [input_data[1]] + ['']*4
                d_item_judge = d_item_judge + ['']*5
            item_name.append(d_item_name)
            item_value.append(d_item_value)
            item_judge.append(d_item_judge)

        if "==Step" in line:
#            print("start process ", line[1:-1], " at line : ", d_no)
            head_name = line.replace('\n','').replace('=','')
            step_s = d_no
            step_e = 0
        elif (step_e==0) and ("==END" in line) : 
#            print("end process ", head_name, " at line : ", d_no-1)
            step_s = 0
            step_e = d_no-1
#            print(item_name, item_value, item_judge)
        elif (step_s + step_e > 0) and (step_e==0) : 
            d_item_name=[]
            d_item_value=[]
            d_item_judge=[]
#            print(input_data, "="*20)
            if line[:1]=="[" :
                ins_type = line.replace('[','').replace(']','')
#                print("ins_type = ", ins_type)
            elif (len(input_data)>1) and (len(ins_type)>1) and (("Val=" in line) and ("(" in line) and (")" in line)) :
                d_item_name = d_item_name + [head_name] + [ins_type] + ['DEFECT_INFO'] + input_data[0].replace('(','').replace(') Val','').replace('S1:','').replace('S2:','').replace('G1:','').replace('G2:','').split(',')
                d_value, d_judge = split_data(input_data[1])
                d_item_value = d_item_value + d_value
                d_item_judge = d_item_judge + d_judge
                item_name.append(d_item_name)
                item_value.append(d_item_value)
                item_judge.append(d_item_judge) 
#                print("ins_result = ", d_item_name, d_item_value, d_item_judge)
            elif (len(input_data)>1) and (len(ins_type)>1) :
                d_item_name = d_item_name + [head_name] + [ins_type] + [input_data[0]] + ['']*4
                d_value, d_judge = split_data(input_data[1])
                d_item_value = d_item_value + d_value
                d_item_judge = d_item_judge + d_judge
                item_name.append(d_item_name)
                item_value.append(d_item_value)
                item_judge.append(d_item_judge)
#                print("ins_result = ", d_item_name, d_item_value, d_item_judge)

        if (line[:7] == "[ESCAPE"): 
#            print("start process ", line[1:-1], " at line : ", d_no)
            head_name = line[1:-1]
            escape_s = d_no
            escape_e = 0
        elif (escape_e==0) and (("==Step" in line) or (len(line)<1) or ((line[:1] == "[") and (line[-1:] == "]"))) : 
#            print("end process ", head_name, " at line : ", d_no-1)
            escape_s = 0
            escape_e = d_no-1
#            print(item_name1, item_value1, item_judge1)
        elif (escape_s + escape_e > 0) and (escape_e==0) : 
            d_item_name=[]
            d_item_value=[]
            d_item_judge=[]
#            print(input_data, "="*20)
            if (len(input_data)==1) and ("~" in line) and ("(" in line) and (")" in line) :
                item_ck = input_data[0].replace('(','').replace(')','').replace(' ','').replace(':','').replace('S','').replace('G','').split(',')
                if len(item_ck) == 1 and ("S:" in line):
                    d_item_name = d_item_name + [head_name] + ['SOURCE'] + ['ESCAPE_INFO'] + item_ck[0].split('~') + ['']*2
                    d_item_value = ['']*5
                    d_item_judge = ['']*5
                if len(item_ck) == 1 and ("G:" in line):
                    d_item_name = d_item_name + [head_name] + ['GATE'] + ['ESCAPE_INFO'] + ['']*2 + item_ck[0].split('~')
                    d_item_value = ['']*5
                    d_item_judge = ['']*5

                elif len(item_ck) == 2:
                    d_item_name = d_item_name + [head_name] + ['ZONE'] + item_ck[0].split('~') + item_ck[1].split('~')
                    d_item_value = ['']*5
                    d_item_judge = ['']*5
                item_name1.append(d_item_name)
                item_value1.append(d_item_value)
                item_judge1.append(d_item_judge) 
#                print("ins_result = ", d_item_name, d_item_value, d_item_judge)
            elif (len(input_data)>1) :
                d_item_name = d_item_name + [head_name] + [input_data[0]] + ['']*5
                d_value, d_judge = split_data(input_data[1])
                d_item_value = d_item_value + d_value
                d_item_judge = d_item_judge + d_judge
                item_name1.append(d_item_name)
                item_value1.append(d_item_value)
                item_judge1.append(d_item_judge)
#                print("ins_result = ", d_item_name, d_item_value, d_item_judge)

        if (line[:7] == "[DRIVER") or (line[:4] == "[CUM") or (line[:6] == "[POWER"): 
#            print("start process ", line[1:-1], " at line : ", d_no)
            head_name = line[1:-1]
            cum_s = d_no
            cum_e = 0
        elif (cum_e==0) and (("==Step" in line) or (len(line)<1) or ((line[:1] == "[") and (line[-1:] == "]"))) : 
#            print("end process ", head_name, " at line : ", d_no-1)
            cum_s = 0
            cum_e = d_no-1
#            print(item_name, item_value, item_judge)
        elif (cum_s + cum_e > 0) and (cum_e==0) : 
            d_item_name=[]
            d_item_value=[]
            d_item_judge=[]
            if(len(input_data))>1:
                d_item_name = d_item_name + [head_name] + split_name(input_data[0])
                d_value, d_judge = split_data(input_data[1])
                d_item_value = d_item_value + d_value
                d_item_judge = d_item_judge + d_judge
            item_name.append(d_item_name)
            item_value.append(d_item_value)
            item_judge.append(d_item_judge)

            
    ck_df1 = pd.DataFrame(data=item_name, 
                            columns=np.core.defchararray.add('item_name_', ['01','02','03', '04', '05', '06', '07']))
    ck_df2 = pd.DataFrame(data=item_value, 
                            columns=np.core.defchararray.add('item_value_', ['01','02','03', '04', '05']))
    ck_df3 = pd.DataFrame(data=item_judge, 
                            columns=np.core.defchararray.add('item_judge_', ['01','02','03', '04', '05']))
    ck_df4 = pd.concat([ck_df1, ck_df2, ck_df3], axis=1)

    ck_df4['PROGRAM_VER'] = PROGRAM_VER
    ck_df4['EQID'] = EQID
    ck_df4['RECIPE_ID'] = RECIPE_ID
    ck_df4['LOT_ID'] = LOT_ID

    if (EQID=='AT02') and (len(AUO_CHIP_ID)>1) and (len(GLASS_ID)>13):
        ck_df4['SHEET_ID'] = GLASS_ID.split('_')[0][:10] + 'P' + GLASS_ID.split('_')[0][11:13]
        ck_df4['CHIP_ID'] = GLASS_ID.split('_')[0][:-2] + AUO_CHIP_ID
        ck_df4['GRADE'] = JUDGEMENT
    elif (len(GLASS_ID)>13):
        ck_df4['SHEET_ID'] = GLASS_ID.split('_')[0][:10] + 'P' + GLASS_ID.split('_')[0][11:13]
        ck_df4['CHIP_ID'] = GLASS_ID.split('_')[0]
        ck_df4['GRADE'] = JUDGEMENT
    else:
        ck_df4['SHEET_ID'] = GLASS_ID.split('_')[0]
        ck_df4['CHIP_ID'] = GLASS_ID.split('_')[0][:-3] + 'T' + GLASS_ID[-2:] + CHIP_ID.replace('.','')
        ck_df4['GRADE'] = GRADE

    ck_df4['CHIP_NO'] = CHIP_ID
    ck_df4['PROC_NO'] = PROC_NO

    ck_df4['LOT_START_TIME'] = LOT_START_DATE + ' ' + LOT_START_TIME
    ck_df4['SHEET_START_TIME'] = GLASS_START_DATE + ' ' + GLASS_START_TIME
    ck_df4['CHIP_START_TIME'] = CHIP_START_DATE + ' ' + CHIP_START_TIME
    ck_df4['CHIP_END_TIME'] = CHIP_END_DATE + ' ' + CHIP_END_TIME

    ck_df4['BIN'] = BIN
    if len(BIN_1)>0:
        ck_df4['BIN_1'] = BIN_1
    else:
        ck_df4['BIN_1'] = BIN
    if len(BIN_2)>0:
        ck_df4['BIN_2'] = BIN_2
    else:
        ck_df4['BIN_2'] = BIN
    ck_df4['RESULT'] = RESULT
    if len(RESULT_1)>0:
        ck_df4['RESULT_1'] = RESULT_1
    else:
        ck_df4['RESULT_1'] = RESULT
    if len(RESULT_2)>0:
        ck_df4['RESULT_2'] = RESULT_2
    else:
        ck_df4['RESULT_2'] = RESULT
    ck_df4['JUDGEMENT'] = JUDGEMENT

    #ck_df4.to_csv(target_path + source_name + '.csv')
    
    return ck_df4

def process_charge_file(map_source_path='./step1_step2.f1000p', map_source_name='step1_step2'):
    file_path = map_source_path #r'C:\Users\hungdechen.AUO\Documents\MT_Yield\input\1716B5\1716B5_Step1_Step2.f1000p'
    filename = map_source_name #'1716B5_Step1_Step2'
    #file_path = os.path.join(directory_name, filename)
    token = file_path.split('.')
    if token[-1][:2] == "f1" : 
        #print('file_path = ', file_path, token)
        in_f100p_a = np.fromfile(file_path,np.uint8)
        len_aa=len(in_f100p_a)
        in_f100p_o = in_f100p_a[0::2]
        in_f100p_e = in_f100p_a[1::2]
        out_f100p_a = 256*in_f100p_o + 1*in_f100p_e
        data = out_f100p_a.copy()
        #print('in_file_shape = ', np.shape(in_f100p_a))

        if len_aa >= 1656*312*2 and len_aa < 1656*313*2:  ##先用此界線分L6K 72*1656 或  L6B 312*1656 , 或許未來會再多幾種
            W=1656
            H=312
            if token[-1]=='f100p':
                C=0.24416883
            if token[-1]=='f10p':
                C=0.024416883
            if token[-1]=='f1p':
                C=0.0024416883
            if token[-1]=='f1000p':
                C=2.4416883
            data=data[5:516677]*C
        if len_aa >= 1440*270*2 and len_aa < 1440*271*2:  ##先用此界線分L6K 72*1656 或  L6B 312*1656 , 或許未來會再多幾種
            W=1440
            H=270
            if token[-1]=='f1000p':
                C=2.4417
            if token[-1]=='f100p':
                C=0.2442
            if token[-1]=='f10p':
                C=0.024416883
            if token[-1]=='f1p':
                C=0.0024416883
            data=data[4:388804]*C
        if len_aa >= 1656*72*2 and len_aa < 1656*73*2:   ##先用此界線分L6K 72*1656 或  L6B 312*1656 , 或許未來會再多幾種
            W=1656
            H=72
            if token[-1]=='f100p':
                C=0.2439715
            if token[-1]=='f10p':
                C=0.02439715
            if token[-1]=='f1p':
                C=0.002439715
            if token[-1]=='f1000p':
                C=2.439715
            data=data[5:119237]*C
        out_f100p_d=np.around(data,1)                 
        out_f100p_2d=np.reshape(data,(H,W))
        out_f100p_1d=np.array(data).flatten().tolist()
    
    return out_f100p_1d,H,W

def get_opid(file_path = 'D:\\MT_YIELD\\input\\970025\\3_BOND_LUM\\UMAOI100_970025_202212171943_LUM_EDC.csv', get_type='OPERATION_ID'):
    check_flag = 0
    skiprows = 0
    remote_file = file_path
    OP_ID = 'NaN'
    with open(remote_file, "rb") as fin:
        for line in fin:
            skiprows += 1 
            try:
                row = line.decode("utf-8").split('\r\n')[0].split(',')[0].split('=')
                #print(skiprows, row)
                if (len(row)>1) & (row[0] == get_type) :
                    OP_ID = row[1]
                    break
                elif row == ['_DATA'] :
                    check_flag = 1
                    break
                elif skiprows>50 :
                    break
            except :
                pass
    fin.close()
    return OP_ID

def job():
    
    CreateLog(fileName='running.log', logPath="./log/")

    logging.warning('start!')
    
    today = date.today()
    d1 = today.strftime("%Y%m%d")
    yesterday = str(int(d1)-1)
    
    files_lst = glob.glob(f"/Dailymongo/data/Source_mt/*/*_LUM_EDC.csv")   
    files_lst = [k for k in files_lst if yesterday in k]

    files_lst = [i.split("/")[-1].split("_")[1]+"_"+i.split("/")[-1].split("_")[2] for i in files_lst]
    files_lst = [word for word in files_lst if len(word.split("_")[0]) <= 7]
    files_lst = list(set(files_lst))
    
    chip_lst = [i.split("_")[0] for i in files_lst]
    time_lst = [i.split("_")[1] for i in files_lst]
    
    sheet_lists = []
    for chip, time in zip(chip_lst,time_lst):
        sheet_lists.append([str(chip),str(time),480,270])
        
    logging.warning(sheet_lists)

    color = {0:'R',1:'G',2:'B'}
    lum_columns = ['CREATETIME','OPID','EQP_ID','MODEL_NO','ABBR_NO','EQP_RECIPE_ID','LED_TYPE','LED_COORDINATE_X','LED_INDEX_I','LED_COORDINATE_Y','LED_INDEX_J','LIGHTING_CHECK','DEFECT_CODE','LED_LUMINANCE','CIE1931_CHROMATICITY_X','CIE1931_CHROMATICITY_Y']
    match_df = pd.DataFrame()

    for sheet_no in range(len(sheet_lists)):
        sheet = sheet_lists[sheet_no][0]
        logging.warning(sheet)
        MT_time = sheet_lists[sheet_no][1]
        #sheet = '9702J2'
        #先由TAR/ENIG是否能找到該SHEET目錄, 再取得AT檔案
        search_key = sheet
        apath = r'/app_1/wma/AMF/data/Target/SW_AT/'
        apattern = '"' + search_key + '"' 
        rawcommand = 'find {path} -name {pattern}'
        command = rawcommand.format(path=apath, pattern=apattern)
        stdin, stdout, stderr = ssh.exec_command(command)
        TYPE_dirs_attr = stdout.read().splitlines()
        AT_ck = 0
        defect_lists = []
        #print(search_key, ' count AT files = ', len(TYPE_dirs_attr), 'AT_ck = ', AT_ck)
        try:
            if len(TYPE_dirs_attr)==1:
                filelist = sftp_client.listdir(apath + search_key + '/')
                for afile in filelist:
                    if afile[-3:].lower() == 'adr' :
                        (head, filename) = os.path.split(str(afile).replace("b'","").replace("'",""))
                        remotefile = filename
                        remotepath = apath + search_key + '/' + filename
                        localpath = './at2mt/at.Adr'
                        sftp_client.get(remotepath, localpath)
                        #print(remotepath, remotefile)
                        adr_df = process_adr(source_path=localpath, source_name='at')
                        adr_df['CHIP_ID'] = sheet
                        adr_df['spec_item'] = adr_df['item_name_03'].str[:5]
                        adr_spec = adr_df[(adr_df['item_name_01'].str[:4]=='Step') & ((adr_df['item_name_03']=='LOWER_LIMIT ')|(adr_df['item_name_03']=='UPPER_LIMIT ')|(adr_df['item_name_03']=='LOWER_LIMIT2 ')|(adr_df['item_name_03']=='UPPER_LIMIT2 '))][['item_name_01','spec_item','item_name_03','item_value_01']].groupby(['item_name_01','spec_item']).agg({'item_value_01': 'max'}).reset_index()
                        defect_adr = adr_df[(adr_df['item_name_03']=='DEFECT_INFO')][['CHIP_ID','PROGRAM_VER','RECIPE_ID','PROC_NO','CHIP_END_TIME','BIN','RESULT','JUDGEMENT','item_name_04','item_name_05','item_name_06','item_name_07','item_name_01','item_name_02','item_value_01']].reset_index(drop=True)
                        for defect_list in defect_adr[['item_name_04','item_name_05','item_name_06','item_name_07','item_name_01','item_name_02','item_value_01','CHIP_ID','PROGRAM_VER','RECIPE_ID','PROC_NO','CHIP_END_TIME','BIN','RESULT','JUDGEMENT']].to_numpy() :
                            SS = int(defect_list[0])
                            SE = int(defect_list[1])
                            GS = int(defect_list[2])
                            GE = int(defect_list[3])
                            for i in range(SS,SE+1):
                                for j in range(GS, GE+1):
                                    defect_lists.append([str(sheet) +'_'+ str(color[(i-1)%3] +'_'+ str(1+(i-1)//3) +'_'+ str(int(sheet_lists[sheet_no][3])-j+1)), str(sheet), MT_time, i, j, color[(i-1)%3], 1+(i-1)//3, int(sheet_lists[sheet_no][3])-j+1, str(defect_list[4]) +'_'+ str(defect_list[5]), defect_list[6], defect_list[8],defect_list[9],defect_list[10],defect_list[11],defect_list[12],defect_list[13],defect_list[14]])
                        if len(defect_adr)>0 :
                            adr_defect = pd.DataFrame(data=defect_lists, columns=['xyz_index','sheet_id','MT_time','Data','Gate','LED_TYPE','x_no','y_no','AT_defect_code','AT_defect_value','AT_PROGRAM_VER','AT_RECIPE_ID','AT_PROC_NO','AT_CHIP_END_TIME','AT_BIN','AT_RESULT','AT_JUDGEMENT']).sort_values(by=['xyz_index', 'AT_defect_code'], ascending=[True, False]).reset_index(drop=True)
                            adr_defect.drop_duplicates(subset='xyz_index', keep='first', inplace=True)
                            adr_defect.set_index('xyz_index', inplace=True)
                            AT_ck = 2
                        else:
                            adr_defect = pd.DataFrame(data=[[str(sheet) + '_0_0_0', sheet,MT_time,0,0,0,0,0,'',0] + adr_df.loc[0,['PROGRAM_VER','RECIPE_ID','PROC_NO','CHIP_END_TIME','BIN','RESULT','JUDGEMENT']].to_list() + [np.NaN,np.NaN,np.NaN,np.NaN,np.NaN]], columns=['xyz_index','sheet_id','MT_time','Data','Gate','LED_TYPE','x_no','y_no','AT_defect_code','AT_defect_value','AT_PROGRAM_VER','AT_RECIPE_ID','AT_PROC_NO','AT_CHIP_END_TIME','AT_BIN','AT_RESULT','AT_JUDGEMENT','LUM_CREATETIME','LUM_OPID','LUM_LED_LUMINANCE','LUM_LIGHTING_CHECK','LUM_DEFECT_CODE'])
                            adr_defect.set_index('xyz_index', inplace=True)
                            AT_ck = 1
                        break
        except:
            adr_defect = pd.DataFrame(data=[[str(sheet) + '_0_0_0', sheet,MT_time,0,0,0,0,0,'',0,'Adr file format mistake',remotepath,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN]], columns=['xyz_index','sheet_id','MT_time','Data','Gate','LED_TYPE','x_no','y_no','AT_defect_code','AT_defect_value','AT_PROGRAM_VER','AT_RECIPE_ID','AT_PROC_NO','AT_CHIP_END_TIME','AT_BIN','AT_RESULT','AT_JUDGEMENT','LUM_CREATETIME','LUM_OPID','LUM_LED_LUMINANCE','LUM_LIGHTING_CHECK','LUM_DEFECT_CODE'])
            adr_defect.set_index('xyz_index', inplace=True)
            AT_ck = 1

        if AT_ck ==0 :
            adr_defect = pd.DataFrame(data=[[str(sheet) + '___', sheet,MT_time,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN,np.NaN]], columns=['xyz_index','sheet_id','MT_time','Data','Gate','LED_TYPE','x_no','y_no','AT_defect_code','AT_defect_value','AT_PROGRAM_VER','AT_RECIPE_ID','AT_PROC_NO','AT_CHIP_END_TIME','AT_BIN','AT_RESULT','AT_JUDGEMENT','LUM_CREATETIME','LUM_OPID','LUM_LED_LUMINANCE','LUM_LIGHTING_CHECK','LUM_DEFECT_CODE'])
            adr_defect.set_index('xyz_index', inplace=True)
            if len(match_df)>0 :
                match_df = pd.concat([match_df, adr_defect], axis=0)
            else:
                match_df = adr_defect.copy() 
        elif AT_ck ==1 :
            if len(match_df)>0 :
                match_df = pd.concat([match_df, adr_defect], axis=0)
            else:
                match_df = adr_defect.copy()
        elif AT_ck ==2 :
            search_key = sheet
            new_lists = []

            # apath = r'/app_1/wma/AMF/data/Source_mt/REAL/'
            apath = r'/app_1/wma/AOI_DATA/L4AFLS01/ledfs/UMAOI100/DATA/UPLOAD/REAL/'          
            apattern = '"*' + search_key + '*_LUM_EDC.*"' 
            rawcommand = 'find {path} -name {pattern}'
            command = rawcommand.format(path=apath, pattern=apattern)
            stdin, stdout, stderr = ssh.exec_command(command)
            filelist = stdout.read().splitlines()          
            for afile in filelist:
                (head, filename) = os.path.split(str(afile).replace("b'","").replace("'",""))
                new_lists.append([sheet, filename, str(afile).replace("b'","").replace("'",""), filename.split('_LUM_EDC.csv')[0].split('_')[-1]])            
            
            # apath = r'/app_1/wma/AMF/data/Source_mt/HIS/'
            apath = r'/app_1/wma/AOI_DATA/L4AFLS01/ledfs/UMAOI100/DATA/UPLOAD/HIS/'
            apattern = '"*' + search_key + '*_LUM_EDC.*"' 
            rawcommand = 'find {path} -name {pattern}'
            command = rawcommand.format(path=apath, pattern=apattern)
            stdin, stdout, stderr = ssh.exec_command(command)
            filelist = stdout.read().splitlines()          
            for afile in filelist:
                (head, filename) = os.path.split(str(afile).replace("b'","").replace("'",""))
                new_lists.append([sheet, filename, str(afile).replace("b'","").replace("'",""), filename.split('_LUM_EDC.csv')[0].split('_')[-1]])
                
            # apath = r'/app_1/wma/AMF/data/Source_mt/NG/'
            apath = r'/app_1/wma/AOI_DATA/L4AFLS01/ledfs/UMAOI100/DATA/UPLOAD/NG/'
            apattern = '"*' + search_key + '*_LUM_EDC.*"' 
            rawcommand = 'find {path} -name {pattern}'
            command = rawcommand.format(path=apath, pattern=apattern)
            stdin, stdout, stderr = ssh.exec_command(command)
            filelist = stdout.read().splitlines()
            for afile in filelist:
                (head, filename) = os.path.split(str(afile).replace("b'","").replace("'",""))
                new_lists.append([sheet, filename, str(afile).replace("b'","").replace("'",""), filename.split('_LUM_EDC.csv')[0].split('_')[-1]])
                
            if len(new_lists)>0 :
                TYPE_dirs_df = pd.DataFrame(data=np.array(new_lists), columns=['sheet', 'remotefile','remotepath','remotetime']).sort_values(by=['sheet', 'remotetime'], ascending=[True, True]).reset_index(drop=True)
                remotepath = TYPE_dirs_df.loc[0,'remotepath']
                localpath = './at2mt/lum.csv'
                sftp_client.get(remotepath, localpath)
                check_flag = 0
                skiprows = 0
                OP_ID = 'NaN'
                with open(localpath, "rb") as fin:
                    for line in fin:
                        skiprows += 1 
                        try:
                            row = line.decode("utf-8").split('\r\n')[0].split(',')[0].split('=')
                            if (len(row)>1) & (row[0] == 'OPERATION_ID') :
                                OP_ID = row[1]
                            elif row == ['_DATA'] :
                                check_flag = 1
                                break
                            elif skiprows>50 :
                                break
                        except :
                            pass
                fin.close()
                if check_flag == 1 :
                    try :
                        partB_df = pd.read_csv(localpath, skiprows=skiprows, header=None, low_memory=False)
                    except :
                        partB_df = pd.read_csv(localpath, skiprows=skiprows, header=None, low_memory=False, encoding='latin1')
                    partB_df = partB_df.iloc[:,0:len(lum_columns)]
                    partB_df.columns = lum_columns
                    partB_df['xyz_index'] = sheet +'_'+ partB_df['LED_TYPE'].astype(str) + '_' + partB_df['LED_INDEX_I'].astype(str) + '_' + partB_df['LED_INDEX_J'].astype(str)
                    partB_df.set_index('xyz_index', inplace=True)
                    partB_df['CREATETIME'] = partB_df['CREATETIME'].astype(str)
                    partB_df['OPID'] = partB_df['OPID'].astype(str)
                    adr_defect.loc[:,'LUM_CREATETIME'] = partB_df.loc[adr_defect.index,'CREATETIME'].to_numpy()
                    adr_defect.loc[:,'LUM_OPID'] = partB_df.loc[adr_defect.index,'OPID'].to_numpy()
                    adr_defect.loc[:,'LUM_LED_LUMINANCE'] = partB_df.loc[adr_defect.index,'LED_LUMINANCE'].to_numpy()
                    adr_defect.loc[:,'LUM_LIGHTING_CHECK'] = partB_df.loc[adr_defect.index,'LIGHTING_CHECK'].to_numpy()
                    adr_defect.loc[:,'LUM_DEFECT_CODE'] = partB_df.loc[adr_defect.index,'DEFECT_CODE'].to_numpy()
                else :
                    adr_defect.loc[:,'LUM_CREATETIME'] = np.NaN
                    adr_defect.loc[:,'LUM_OPID'] = np.NaN
                    adr_defect.loc[:,'LUM_LED_LUMINANCE'] = np.NaN
                    adr_defect.loc[:,'LUM_LIGHTING_CHECK'] = np.NaN
                    adr_defect.loc[:,'LUM_DEFECT_CODE'] = np.NaN
                if len(match_df)>0 :
                    match_df = pd.concat([match_df, adr_defect], axis=0)
                else:
                    match_df = adr_defect.copy()
        else:
            adr_defect = []
    logging.warning("df")
    logging.warning(match_df)
    if len(match_df)>0 :
        match_df['MT_time'] = match_df['MT_time'].str[:10]
        match_df['AT2LUM_Match'] = match_df['LUM_LIGHTING_CHECK']
        match_df.loc[match_df['AT2LUM_Match'].isna()==True, 'AT2LUM_Match']=''
        match_df.loc[match_df['AT2LUM_Match']==0.0, 'AT2LUM_Match']='Y'
        match_df.loc[match_df['AT2LUM_Match']==1.0, 'AT2LUM_Match']='N'
        match_df['AT_defect'] = 1
        match_df.loc[match_df['AT_RECIPE_ID'].isna()==True, 'AT_RECIPE_ID']=''
        match_df.loc[match_df['AT_CHIP_END_TIME'].isna()==True, 'AT_CHIP_END_TIME']=''
        match_df.loc[match_df['LUM_CREATETIME'].isna()==True, 'LUM_CREATETIME']=''
        match_df.loc[match_df['LUM_OPID'].isna()==True, 'LUM_OPID']=''
        compare_df = match_df.groupby(['sheet_id','MT_time','AT_RECIPE_ID','AT_CHIP_END_TIME','LUM_CREATETIME','LUM_OPID','AT2LUM_Match','LED_TYPE']).agg({'AT_defect': 'count'}).reset_index()
        compare_df.loc[compare_df['AT2LUM_Match']=='', 'AT2LUM_Match']='AT_NoDefect_Sheet'
        compare_df.loc[compare_df['AT_RECIPE_ID']=='', 'AT2LUM_Match']='AT_NoData_Sheet'
        compare_df.loc[compare_df['AT2LUM_Match']=='Y', 'AT2LUM_Match']='Match_Y'
        compare_df.loc[compare_df['AT2LUM_Match']=='N', 'AT2LUM_Match']='Match_N'
        compare_df = compare_df.pivot_table(values='AT_defect', index=['sheet_id','MT_time','AT_RECIPE_ID','AT_CHIP_END_TIME','LUM_CREATETIME','LUM_OPID','LED_TYPE'], columns='AT2LUM_Match', aggfunc='sum').reset_index().sort_values(by=['MT_time','sheet_id'], ascending=[False,False]).reset_index(drop=True)
        compare_lists = ['sheet_id']
        if np.isin(['Match_N'], compare_df.columns.to_list()) :
            compare_df.loc[compare_df['Match_N'].isna()==True, 'Match_N']=0
            compare_lists.append('Match_N')
        if np.isin(['Match_Y'], compare_df.columns.to_list()) :
            compare_df.loc[compare_df['Match_Y'].isna()==True, 'Match_Y']=0
            compare_lists.append('Match_Y')
        if np.isin(['AT_NoData_Sheet'], compare_df.columns.to_list()) :
            compare_df.loc[compare_df['AT_NoData_Sheet'].isna()==True, 'AT_NoData_Sheet']=0
            compare_lists.append('AT_NoData_Sheet')
        if np.isin(['AT_NoDefect_Sheet'], compare_df.columns.to_list()) :
            compare_df.loc[compare_df['AT_NoDefect_Sheet'].isna()==True, 'AT_NoDefect_Sheet']=0
            compare_lists.append('AT_NoDefect_Sheet')
        if np.isin(['Match_N'], compare_df.columns.to_list()) :
            compare_df.loc[compare_df['Match_N'].isna()==True, 'Match_N']=0
        if np.isin(['Match_Y'], compare_df.columns.to_list()) :
            compare_df.loc[compare_df['Match_Y'].isna()==True, 'Match_Y']=0
        if np.isin(['AT_NoDefect_Sheet'], compare_df.columns.to_list()) :
            compare_df.loc[compare_df['AT_NoDefect_Sheet'].isna()==True, 'AT_NoDefect_Sheet']=0
        if np.isin(['AT_NoData_Sheet'], compare_df.columns.to_list()) :
            compare_df.loc[compare_df['AT_NoData_Sheet'].isna()==True, 'AT_NoData_Sheet']=0
        compare_dir = {}
        for compare_list in compare_lists :
            if compare_list == 'AT_NoData_Sheet':
                compare_dir['AT_NoData_Sheet'] = 'sum'
            if compare_list == 'AT_NoDefect_Sheet':
                compare_dir['AT_NoDefect_Sheet'] = 'sum'
            if compare_list == 'sheet_id':
                compare_dir['sheet_id'] = 'count'
            if compare_list == 'Match_N':
                compare_dir['Match_N'] = 'sum'
            if compare_list == 'Match_Y':
                compare_dir['Match_Y'] = 'sum' 
        compare_day = compare_df.groupby(['MT_time']).agg(compare_dir).reset_index()
        if np.isin(['AT_NoData_Sheet'], compare_lists) == False :
            compare_day['AT_NoData_Sheet'] = 0
        if np.isin(['AT_NoDefect_Sheet'], compare_lists) == False :
            compare_day['AT_NoDefect_Sheet'] = 0
        if np.isin(['Match_N'], compare_lists) == False :
            compare_day['Match_N'] = 0
        if np.isin(['Match_Y'], compare_lists) == False :
            compare_day['Match_Y'] = 0  
        compare_day['AT_Match_Sheet'] = compare_day['sheet_id'] - compare_day['AT_NoData_Sheet'] - compare_day['AT_NoDefect_Sheet']
        compare_day = compare_day[['MT_time','AT_NoData_Sheet','AT_NoDefect_Sheet','AT_Match_Sheet','Match_N','Match_Y']].sort_values(by=['MT_time'], ascending=[False]).reset_index(drop=True)
        compare_day.columns = ['MT_time','AT_NoData_sheet','AT_NoDefect_sheet','AT_Match_Sheet','Match_N_defect','Match_Y_defect']
        # delete the index name
        compare_df = compare_df.rename_axis(None, axis=1)
        
        # link to the server
        client = MongoClient('mongodb://wma:mamcb1@10.88.26.102:27017')
        db = client["MT"]

        # AT2MT
        logging.warning("AT2MT start append")
        collection = db["AT2MT"] 
        result = match_df.to_json(orient="records")
        parsed = json.loads(result)  
        collection.insert_many(parsed)               
        logging.warning("AT2MT finish append")
        
        # AT2MT_SUMMARY 
        logging.warning("AT2MT_SUMMARY start append")
        collection = db["AT2MT_SUMMARY"] 
        result = compare_df.to_json(orient="records")
        parsed = json.loads(result)  
        collection.insert_many(parsed)    
        cursor = collection.find({})
        df = pd.DataFrame.from_records(cursor)

        df = df.dropna(subset=['AT_CHIP_END_TIME'])
        df = df[df.AT_CHIP_END_TIME != ""]
        df1 = df[df['MT_time'].str.contains("/", na=False)]
        df1['MT_time'] = df1['MT_time'].str.replace("/","")
        df2 = df[~df['MT_time'].str.contains("/", na=False)]
        df = pd.concat([df1,df2])
        df = df.drop_duplicates(subset=df.columns.difference(['_id']))

        db.drop_collection("AT2MT_SUMMARY")
        collection = db["AT2MT_SUMMARY"] 
        result = df.to_json(orient="records", default_handler=str)
        parsed = json.loads(result)  
        collection.insert_many(parsed)  
        logging.warning("AT2MT_SUMMARY finish append")        
        
    else:
        
        logging.warning('no new data!')

if __name__ == '__main__':
    
    # 連結 sftp
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname="10.88.19.29", port=22, username="wma", password="wma")
    sftp_client = ssh.open_sftp()
    
    # job()    
    schedule.every().day.at("07:30").do(job)  

    while True:  
        schedule.run_pending() 
        time.sleep(1) # wait one minute           
