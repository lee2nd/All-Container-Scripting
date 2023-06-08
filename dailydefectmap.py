import pandas as pd
import numpy as np
import schedule
from datetime import date, timedelta, datetime
import time
from pymongo import MongoClient 
import os
import logging
import plotly.graph_objects as go

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
        
def color_trans(df):
    
    df["LED_TYPE"] = df["LED_TYPE"].astype(str).str.replace("R","red")
    df["LED_TYPE"] = df["LED_TYPE"].astype(str).str.replace("G","green")
    df["LED_TYPE"] = df["LED_TYPE"].astype(str).str.replace("B","blue")
    
    return df

def plotly_multi_ver(fig,df,type,color):

    if type == "lum":
        x = "Pixel_X"
        y = "Pixel_Y"
    elif type == "at":
        x = "x_no"
        y = "y_no"     

    df = df[df["LED_TYPE"]==color]

    x_lst = np.array(df[x].to_list())
    y_lst = np.array(df[y].to_list())

    if type == "lum":
        fig.add_trace(go.Scatter(
            x=x_lst,
            y=y_lst,
            name=f"{type} - {color}",
            mode='markers',
            marker=dict(
                size=6,
                color=f'{color}',
                line=dict(
                    color=f'{color}',
                    width=1
                ),
                symbol='square-open'
            )
        ))  
    elif type == "at":
        fig.add_trace(go.Scatter(
            x=x_lst,
            y=y_lst,
            name=f"{type} - {color}",
            mode='markers',
            marker=dict(
                size=5,
                color=f'{color}',
                line=dict(
                    color='DarkSlateGrey',
                    width=2
                ),
                symbol='circle'
            )
        )) 

def job(duration):
    
    today = date.today()
    d_today = today.strftime("%Y%m%d")
    d_start = int((datetime.now() - timedelta(days=duration)).strftime("%Y%m%d") + "00")
    d_end = int(d_today + "00")

    # connect the server
    client = MongoClient('mongodb://wma:mamcb1@10.88.26.102:27017')
    db = client["MT"]
    
    # LUM
    collection = db["AOI_LUM_Defect_Coordinates"] 
    cursor = collection.find({"CreateTime": {'$gt':str(d_start),'$lt':str(d_end)}})
    lum_df = pd.DataFrame.from_records(cursor)  
    
    chip_list = list(set(list(lum_df.SHEET_ID)))
   
    if len(chip_list) > 0:
            
        for chipid in chip_list:
            
            logging.warning(chipid + " : finished")
            
            # Build figure
            fig = go.Figure()
            
            # 渲染 AT
            collection = db["AT2MT"] 
            cursor = collection.find({"sheet_id":f"{chipid}"})
            df_at = pd.DataFrame.from_records(cursor)
            if len(df_at)>0:
                df_at = df_at[["sheet_id","x_no","y_no","LED_TYPE"]]
                df_at = color_trans(df_at)
                plotly_multi_ver(fig,df_at,"at","red")
                plotly_multi_ver(fig,df_at,"at","green")
                plotly_multi_ver(fig,df_at,"at","blue")     
            
            # 渲染 LUM
            collection = db["AOI_LUM_Defect_Coordinates"] 
            cursor = collection.find({"SHEET_ID":f"{chipid}"})
            df_lum = pd.DataFrame.from_records(cursor)
            if len(df_lum)>0:

                df_lum = df_lum[["SHEET_ID","Pixel_X","Pixel_Y","LED_TYPE","Insepction_Type"]]
                df_lum = df_lum[df_lum["Insepction_Type"]=="L255"]
                df_lum = df_lum[["SHEET_ID","Pixel_X","Pixel_Y","LED_TYPE"]]
                df_lum = color_trans(df_lum)
                plotly_multi_ver(fig,df_lum,"lum","red")
                plotly_multi_ver(fig,df_lum,"lum","green")
                plotly_multi_ver(fig,df_lum,"lum","blue")                

            # # 設定 x 和 y 軸上下界
            fig.update_xaxes(range = [0,480])
            fig.update_yaxes(range = [0,270])

            fig.update_layout(title=f"{chipid}", width=1500, height=900)
            fig.write_html(f"./output/{chipid} defect map.html") 
            
    else:
        logging.warning("no data")

if __name__ == '__main__':
    
    # job(1)
    schedule.every().day.at("05:30").do(lambda: job(1))  

    while True:  
        schedule.run_pending() 
        time.sleep(1)        
