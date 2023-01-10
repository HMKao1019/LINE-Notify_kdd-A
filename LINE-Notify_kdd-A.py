# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 10:56:46 2023

@author: hmkao
"""

# In[] Part I: Import the third-party modules

#-- SQL modules
import mysql.connector

#-- Time modules
from datetime import datetime,timezone,timedelta
import time

#-- Other modules
import os
import json
import requests


# In[] Part II: Read KEYS

#-- Set execuation path on VM of GCP
execuation_Path = os.getcwd()
git_folderName = '/LINE-Notify_kdd-A' 
 
#-- Read KEYS
with open (execuation_Path + git_folderName + '/keys.json', 'r') as js:
    key = json.loads(js.read())


# In[] Part III: Some useful functions

#-- Show Taipei Time
def showTaipeiTime():
    
    dt = datetime.utcnow()
    dt = dt.replace(tzinfo=timezone.utc)
    tzutc_8 = timezone(timedelta(hours=8))
    local_dt = dt.astimezone(tzutc_8).strftime('%Y-%m-%d %H:%M:%S') #- format: 2022-10-26 15:41:07
    
    return local_dt


# In[] Part IV: Connect to SQL

#-- (0) Turn on database connection
def turnOnDBConnection():
    try:
        db_host = key["host"]
        db_user = key["user"]
        db_password = key["passwd"]
        db_name = key["database"]

        mydb = mysql.connector.connect(host = db_host, user = db_user, password = db_password, db = db_name)
                
    except mysql.connector.Error as err:
        #-- Unable to connect to the database, i.e. pass 
        print(str(err.args))     
        pass
    
    else:
        return mydb
    

#-- (1) query function --> line_notify_queue / line_notify_log
def queryTableRow(tableName, column):
    #-- Turn on database connection  
    mydb = turnOnDBConnection()
    
    #-- mydb objectification
    mycursor = mydb.cursor() 
            
    #-- execute instruction: database / table
    mycursor.execute("SELECT " + column + " FROM " + tableName + ";")

    #-- fetchall   
    queue_info = mycursor.fetchall()
        
    #-- Turn off database connection
    mydb.close()
        
    return queue_info


#-- (2) insert function  --> line_notify_log
def insertTableRow(tableName, var1, var2, var3, var4, var5, var6, var7, var8, var9):
    #-- Turn on database connection  
    mydb = turnOnDBConnection()

    #-- mydb objectification
    mycursor= mydb.cursor() 
                    
    sql= "INSERT INTO " + tableName + " (sn, message_sn, send_to, content, sent_ok, created_time, scheduled_time, notified_time, remark) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val= (var1, var2, var3, var4, var5, var6, var7, var8, var9) 
       
    #-- execute instruction
    mycursor.execute(sql, val) 
                
    #-- commit
    mydb.commit()
                
    #-- Turn off database connection
    mydb.close()    


#-- (3) delete function --> line_notify_queue 
def deleteTableRow(tableName, message_sn):
    #-- Turn on database connection  
    mydb = turnOnDBConnection()
    
    #-- mydb objectification
    mycursor = mydb.cursor() 
            
    #-- execute instruction: database / table / message_sn
    mycursor.execute("DELETE FROM " + tableName + " WHERE message_sn = " + str(message_sn) + ";")
    
    #-- commit
    mydb.commit()
        
    #-- Turn off database connection
    mydb.close()


#-- (4) Re-insert function  --> line_notify_queue
def ReInsertTableRow(tableName, var1, var2, var3, var4, var5, var6):
    #-- Turn on database connection  
    mydb = turnOnDBConnection()

    #-- mydb objectification
    mycursor= mydb.cursor() 
                    
    sql= "INSERT INTO " + tableName + " (message_sn, send_to, content, created_time, scheduled_time, expired_time) VALUES (%s, %s, %s, %s, %s, %s)"
    val= (var1, var2, var3, var4, var5, var6) 
       
    #-- execute instruction
    mycursor.execute(sql, val) 
                
    #-- commit
    mydb.commit()
                
    #-- Turn off database connection
    mydb.close() 


# In[] Part V: Connect to Line Notify "AND" Execute notified (push) messages

#-- Connect to "Line Notify"    
url = 'https://notify-api.line.me/api/notify'
token = key['token']
headers = {'Authorization': 'Bearer ' + token}   # 設定權杖


#-- query notified data and parse it
results = queryTableRow(key["queueTable"], '*')

for i in range(len(results)):
    
    message_sn =     results[i][0]  # type: int
    send_to =        results[i][1]  # type: str
    content =        results[i][2]  # type: str
    created_time =   results[i][3]  # type: datetime
    scheduled_time = results[i][4]  # type: datetime
    expired_time =   results[i][5]  # type: datetime
    
    
    #-- Convert to timestamp for comparison
    scheduled_struct = time.strptime(str(scheduled_time), "%Y-%m-%d %H:%M:%S") # convert to time tuple
    scheduled_timestamp = int(time.mktime(scheduled_struct)) # convert to timestamp / type: int
    
    expired_struct = time.strptime(str(expired_time), "%Y-%m-%d %H:%M:%S") 
    expired_timestamp = int(time.mktime(expired_struct)) 
    
    notified_time = showTaipeiTime()
    notified_struct = time.strptime(str(notified_time), "%Y-%m-%d %H:%M:%S") 
    notified_timestamp = int(time.mktime(notified_struct)) 
    
    
    #-- Connect to "Line Notify" and notify (push) messages
    
    # Organize your send list
    send_to_list = send_to.split(',')
    failed_send_to_list = []
    success_send_to_list = []
    
    #-- Sent within the valid period
    if notified_timestamp >= scheduled_timestamp and notified_timestamp <= expired_timestamp:
            
        for i in range(len(send_to_list)):
            # notify messages              
            data = {'message': '\n' + str(content)}  
            
            if send_to_list[i] == key['sent_to']:
                # Use the POST method
                notify_message = requests.post(url, headers = headers, data = data)   
                # record success send list
                success_send_to_list.append(send_to_list[i])
                
            else:
                # log failed send list
                failed_send_to_list.append(send_to_list[i])
 
                       
        #- All users are notified.
        if len(failed_send_to_list) == 0:
            
            #-- delete notified data from "line_notify_queue" based on "message_sn"
            deleteTableRow(key["queueTable"], message_sn)
            
            # confirm log number
            sn = len(queryTableRow(key["logTable"], 'sn')) + 1
            
            # confirm notified status
            sent_ok = True
            
            # write remark   
            remark = ''
            
            # organize all send ids
            sent_to_all_id = ''
            for i in range(len(success_send_to_list)):
                sent_to_all_id = sent_to_all_id + str(success_send_to_list[i].strip()) + ', '
            
            # insert notified data to "line_notify_log"
            insertTableRow(key["logTable"], sn, message_sn, sent_to_all_id, content, sent_ok, created_time, scheduled_time, notified_time, remark) 
     
            
        #- All users are not notified.
        elif len(failed_send_to_list) == len(send_to_list):
            pass
        
        
        #- Some users have been notified, others have not.
        else:
            
            # delete notified data from "line_notify_queue" based on "message_sn"
            deleteTableRow(key["queueTable"], message_sn)
            
            # re-insert notified data to "line_notify_queue" due to some unsuccessful notification to specific users
            
            # organize re-send ids
            re_sent_to = ''
            for i in range(len(failed_send_to_list)):
                re_sent_to = re_sent_to + str(failed_send_to_list[i].strip()) + ', '
            
            # re-insert notified data to "line_notify_queue"
            ReInsertTableRow(key["queueTable"], message_sn, re_sent_to, content, created_time, scheduled_time, expired_time)

    
            # insert notified data to "line_notify_log"
            
            # organize some send ids
            sent_to_some_id = ''
            for i in range(len(success_send_to_list)):
                sent_to_some_id = sent_to_some_id + str(success_send_to_list[i].strip()) + ', '
            
            # confirm log number
            sn = len(queryTableRow(key["logTable"], 'sn')) + 1
            
            # confirm notified status
            sent_ok = True        
            
            # write remark    
            remark = 'Content has benn sent to some users ' + sent_to_some_id + ' within the valid period.'
            
            # insert notified data to "line_notify_log"
            insertTableRow(key["logTable"], sn, message_sn, sent_to_some_id, content, sent_ok, created_time, scheduled_time, notified_time, remark) 

         
    #-- Exceed expired time
    elif notified_timestamp >= expired_timestamp:
        
        # delete notified data from "line_notify_queue" based on "message_sn"
        deleteTableRow(key["queueTable"], message_sn)
        
        # insert notified data to "line_notify_log" 
        
        # confirm log number
        sn = len(queryTableRow(key["logTable"], 'sn')) + 1
        
        # confirm notified status
        sent_ok = False
        
        # write remark
        remark = 'Content could not be sent to all users due to expiration.'
        
        # insert notified data to "line_notify_log"
        insertTableRow(key["logTable"], sn, message_sn, send_to, content, sent_ok, created_time, scheduled_time, notified_time, remark) 
