import zeep
import pandas as pd
import base64
import schedule
import paramiko
from datetime import date, timedelta, datetime
import time
from pymongo import MongoClient 

class auto_mail():
    def __init__(self):
        # 收件者 & cc
        self.Rmail_user = 'Webb.Chu@auo.com; Doran.Wu@auo.com'
        self.Cmail_user = 'Frank.Lee@auo.com; HungDe.Chen@auo.com'        
        # self.Rmail_user = 'Frank.Lee@auo.com'
        # self.Cmail_user = 'Frank.Lee@auo.com'

        self.client = zeep.Client("http://ids.cdn.corpnet.auo.com/IDS_WS/Mail.asmx?wsdl") 
        # 連到 server
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect("10.88.19.29", 22, "wma", "wma")
        self.sftp_client = ssh.open_sftp()

    def sendReport(self, filepath, df_summary_sheet, duration, d_start, d_end):

        filename = filepath.split("/")[-1]
        with open(filepath, "rb") as f:
            encoded_string = base64.b64encode(f.read())
            encoded_execl_file = encoded_string.decode('utf-8')

        summary_sheet_html_table = df_summary_sheet.to_html(index=False)
        
        if duration == 1:
            title = "Daily"        
        elif duration == 7:
            title = "Weekly"        
        elif duration == 30:
            title = "Monthly"            
        
        ManualSend_01 = {
                'strMailCode': 'OOt4AZp0pAo=', # MailCode
                'strRecipients':'' +  self.Rmail_user + '', 
                'strCopyRecipients':'' + self.Cmail_user + '', # 副本
                'strSubject': f'{title} Comapre Report at ' + d_start + " to " + d_end , # 標題
                'strBody':
                '<font face="times new roman">' +
                'Hi All: '+
                '</font>' +
                '<br><br><font face="times new roman">' +
                summary_sheet_html_table + 
                '', 
                'strFileBase64String': filename + ':' + str(encoded_execl_file) + 
                ''
        }
        response_01 = self.client.service.ManualSend_39(**ManualSend_01)
            
        print('Send report mail successful or not: ' + str(response_01))

def job(duration):
    
    today = date.today()
    d_today = today.strftime("%Y%m%d")
    d_start = int((datetime.now() - timedelta(days=duration)).strftime("%Y%m%d") + "00")
    d_end = int(d_today + "00")

    # connect the server
    client = MongoClient('mongodb://wma:mamcb1@10.88.26.102:27017')
    db = client["MT"]
    
    # AT2MT
    collection = db["AT2MT"] 
    cursor = collection.find({"MT_time": {'$gt':str(d_start),'$lt':str(d_end)}})
    match_df = pd.DataFrame.from_records(cursor)

    # AT2MT_SUMMARY
    collection = db["AT2MT_SUMMARY"] 
    cursor = collection.find({"MT_time": {'$gt':d_start,'$lt':d_end}})
    compare_df = pd.DataFrame.from_records(cursor)
        
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(f'./output/compare({duration}).xlsx', engine='xlsxwriter')
    
    # delete the index name
    match_df = match_df.iloc[:,1:]
    compare_df = compare_df.iloc[:,2:]
    
    # sort the df by MT time
    match_df = match_df.sort_values('MT_time')
    compare_df = compare_df.sort_values('MT_time')
    
    # Write each dataframe to a different worksheet.
    compare_df.to_excel(writer, sheet_name='summary by sheet', index=False)
    match_df.to_excel(writer, sheet_name='raw', index=False)

    # Close the Pandas Excel writer and output the Excel file.
    writer.close()    
    
    time.sleep(1)        
    
    obj = auto_mail()
    obj.sendReport(filepath=f"./output/compare({duration}).xlsx", df_summary_sheet=compare_df, duration=duration, d_start=str(d_start), d_end=str(d_end))
    
if __name__ == '__main__':
    
    # job(1)
    # job(7)
    # job(30)
    schedule.every().day.at("07:30").do(lambda: job(1))  
    schedule.every().monday.at("07:30").do(lambda: job(7))
    schedule.every().monday.at("07:30").do(lambda: job(30))

    while True:  
        schedule.run_pending() 
        time.sleep(1)        
