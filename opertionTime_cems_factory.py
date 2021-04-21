# -*- coding: utf-8 -*-
"""
Created on Tue Apr 13 09:36:56 2021

@author: bmeyj
"""


import pymysql
import pandas as pd
import time
import datetime
import numpy as np
from dateutil import parser
import copy
from flask import Flask, jsonify,url_for
from flask import abort
from flask import request
from response import ResMsg
from flask_apscheduler import APScheduler
import pika
import json
import happybase
# from hbase_getdata import generate_data

class Config:
    """App configuration."""

    SCHEDULER_API_ENABLED = True


scheduler = APScheduler()

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False #正确显示中文字符


@app.route('/bme/api/cemscal/bydevice/<int:customer_id>', methods=['GET'])
def get_task_cemsbydevice(customer_id):
    res = ResMsg()
#检查tasks内部的元素，是否有元素的id值和参数相匹配
    sync = list(filter(lambda t: t['customer_id'] == customer_id,cems_by_device_dict))
 #有的话，就返回列表形式包裹的这个元素，没有的话就返回error cod0000000000000e
    res.update(data=sync)  
    # if len(cmes) == 0:
    #     abort(404)
    return jsonify(res.data)
    # return jsonify({'cmes': cmes[0]})
    #否则，将这个cmes以json的格式返回。
@app.route('/bme/api/cemscal/bysubfactory/<int:customer_id>', methods=['GET'])
def get_task_cemsbysubfactory(customer_id):
    res = ResMsg()
#检查tasks内部的元素，是否有元素的id值和参数相匹配
    sync = list(filter(lambda t: t['customer_id'] == customer_id,cems_by_subfactory_dict))
 #有的话，就返回列表形式包裹的这个元素，没有的话就返回error cod0000000000000e
    res.update(data=sync)  
    # if len(cmes) == 0:
    #     abort(404)
    return jsonify(res.data)
    # return jsonify({'cmes': cmes[0]})
    #否则，将这个cmes以json的格式返回。 
@app.route('/bme/api/cemscal/byfactory/<int:customer_id>', methods=['GET'])
def get_task_cemsbyfactory(customer_id):
    res = ResMsg()
#检查tasks内部的元素，是否有元素的id值和参数相匹配
    sync = list(filter(lambda t: t['customer_id'] == customer_id,cems_by_factory_dict))
 #有的话，就返回列表形式包裹的这个元素，没有的话就返回error cod0000000000000e
    res.update(data=sync)  
    # if len(cmes) == 0:
    #     abort(404)
    return jsonify(res.data)
    # return jsonify({'cmes': cmes[0]})
    #否则，将这个cmes以json的格式返回。     


# def make_public_task(json,types):       
#     new_task={}             #新建一个对象，字典类型
#     method = 'get_task_product' if types == 'product' else 'get_task_duster'
#     for key in json:        #遍历字典内部的KEY  
#         if key == 'customer_id': #当遍历到id的时候，为新对象增加uri的key，对应的值为完整的uri  
#             new_task['uri'] = url_for(method,customer_id=json['customer_id'],data_dict=json,_external=True)  
#         else:  
#             new_task[key] = json[key]  #其他的key，分别一一对应加入新对象  
#     return new_task                            #最后返回新对象数据  


# #修改获取合集的路由

# @app.route('/bme/api/cemscal/bydevice',methods=['GET'])  
# def get_tasks_device():  
#     return jsonify({'tasks': list(map(make_public_task,cems_by_device_dict,'cemsbydevice'))})  #使用map函数，罗列出所有的数据，返回的数据信息，是经过辅助函数处理的 
# @app.route('/bme/api/cemscal/bysubfactory',methods=['GET'])  
# def get_tasks_subfactory():  
#     return jsonify({'tasks': list(map(make_public_task,cems_by_subfactory_dict,'cemsbysubfactory'))})  #使用map函数，罗列出所有的数据，返回的数据信息，是经过辅助函数处理的 
# @app.route('/bme/api/cemscal/byfactory',methods=['GET'])  
# def get_tasks_factory():  
#     return jsonify({'tasks': list(map(make_public_task,cems_by_factory_dict,'cemsbyfactory'))})  #使用map函数，罗列出所有的数据，返回的数据信息，是经过辅助函数处理的 

# @app.route('/bme/api/opertime/duster',methods=['GET'])  
# def get_tasks_factory():  
#     return jsonify({'tasks': list(map(make_public_task,duster_dict_factory,'duster'))})  






class Hbase():
    def __init__(self,ip,port=9090):
        self.conn = happybase.Connection(ip,port)
        self.conn.open()
    
    def getSignal(self,tablename,signalID,starttime,endtime):
        self.table = self.conn.table(tablename)
        start = signalID+starttime
        end = signalID+endtime
        dic = {}
        try:
            for key, value in self.table.scan(row_start=start,row_stop=end):
                dic[key.decode()] = dict(map(lambda x: (x[0].decode(), x[1].decode()), value.items()))
        except:
            dic = {}
        self.conn.close()
        return dic
class database():
    def __init__(self,ip,port,user,passwd,dbname):
        self.conn = pymysql.connect(host=ip,  
                                    port = port,
                        user=user,  # 数据库用户名
                        passwd=passwd,  # 数据库密码
                        db=dbname # 数据库名称
                        )
        self.cursor = self.conn.cursor()
        
    
    def getData(self,sql):
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        columnDes = self.cursor.description
        colName = [columnDes[i][0] for i in range(len(columnDes))]#获取数据库表中表头名称
        df = pd.DataFrame([list(i) for i in data],columns=colName)#表头写入DF
        return df
# @scheduler.task("cron", id="cemsbyfactory_1", hour='*')
def generate_data():
    signal_so2 = database('172.31.235.64', 3306, 'bme', 'Bme@709394', 'basic_data').getData('SELECT\
                                                                                            	ts.signal_no,\
                                                                                            	ts.signal_name,\
                                                                                            	ts.customer_id,\
                                                                                            	tds.device_no,\
                                                                                            	ts.signal_type \
                                                                                            FROM\
                                                                                            	`t_signal` ts\
                                                                                            	LEFT JOIN t_device_signal tds ON ts.signal_no = tds.signal_no \
                                                                                            WHERE\
                                                                                            	ts.category_id = 17')
    signal_nox = database('172.31.235.64', 3306, 'bme', 'Bme@709394', 'basic_data').getData('SELECT\
                                                                                            	ts.signal_no,\
                                                                                            	ts.signal_name,\
                                                                                            	ts.customer_id,\
                                                                                            	tds.device_no,\
                                                                                            	ts.signal_type \
                                                                                            FROM\
                                                                                            	`t_signal` ts\
                                                                                            	LEFT JOIN t_device_signal tds ON ts.signal_no = tds.signal_no \
                                                                                            WHERE\
                                                                                            	ts.category_id = 18')
    signal_tdc = database('172.31.235.64', 3306, 'bme', 'Bme@709394', 'basic_data').getData('SELECT\
                                                                                            	ts.signal_no,\
                                                                                            	ts.signal_name,\
                                                                                            	ts.customer_id,\
                                                                                            	tds.device_no,\
                                                                                            	ts.signal_type \
                                                                                            FROM\
                                                                                            	`t_signal` ts\
                                                                                            	LEFT JOIN t_device_signal tds ON ts.signal_no = tds.signal_no \
                                                                                            WHERE\
                                                                                            	ts.category_id = 19')
    signal_gasflow = database('172.31.235.64', 3306, 'bme', 'Bme@709394', 'basic_data').getData('SELECT\
                                                                                            	ts.signal_no,\
                                                                                            	ts.signal_name,\
                                                                                            	ts.customer_id,\
                                                                                            	tds.device_no,\
                                                                                            	ts.signal_type \
                                                                                            FROM\
                                                                                            	`t_signal` ts\
                                                                                            	LEFT JOIN t_device_signal tds ON ts.signal_no = tds.signal_no \
                                                                                            WHERE\
                                                                                            	ts.category_id = 113 \
                                                                                            and tds.device_no in \
                                                                                            (SELECT device_no From t_device WHERE type_id  = 1539730750575002)')   
    device_subfactory = database('172.31.235.64', 3306, 'bme', 'Bme@709394', 'basic_data').getData ('SELECT device_no,device_name,branch_factory,customer_id \
                                                                                                    From t_device ')                                                                                      
                                                                                                
                                                                                                
                                                                                                
                                                                                                
    start_hour = str(int(time.mktime(time.strptime((datetime.datetime.now()-datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:00:00'), "%Y-%m-%d %H:%M:%S"))))
    end_hour = str(int(time.mktime(time.strptime((datetime.datetime.now()-datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:59:59'), "%Y-%m-%d %H:%M:%S"))))
    cems_device_so2 = pd.DataFrame()
    cems_device_nox = pd.DataFrame()
    cems_device_tdc = pd.DataFrame()
    cems_device_gasflow = pd.DataFrame()
    ####################################################so2#########################################################################################
    for cid in signal_so2['customer_id'].unique():#so2
        print(cid)
        tempdf = signal_so2[signal_so2['customer_id'] == cid].reset_index(drop = True)
        tempdf = tempdf.loc[~tempdf['signal_name'].str.contains('进口')].reset_index(drop = True)
        tempdf = tempdf.loc[~tempdf['signal_name'].str.contains('入口')].reset_index(drop = True)
        df_so2 = pd.DataFrame()
        for i in range(len(tempdf)):
            signal_id = str(tempdf['signal_no'][i])
            device_no = str(tempdf['device_no'][i])
            dic = Hbase('172.31.235.67').getSignal('exceed', signal_id, start_hour, end_hour)
            df = pd.DataFrame.from_dict(dic,orient='index').reset_index().rename(columns={'index':'signal_no','f1:val':'so_2'})
            df['customer_id'] = cid
            df['device_no'] = device_no
            # df =  pd.merge(df, device_subfactory[["device_no","device_name",'branch_factory']],on = "device_no",how = "left")
            df_so2 = df_so2.append(df)
        cems_device_so2 = cems_device_so2.append(df_so2)
    cems_device_so2['so_2'] = cems_device_so2['so_2'].astype(float)
    cems_device_so2['customer_id'] = cems_device_so2['customer_id'].astype(int)
    cems_device_so2 = cems_device_so2[['so_2','device_no','customer_id']].groupby(['device_no','customer_id']).mean().reset_index()
    ##########################################################nox###################################################################################    
    for cid in signal_nox['customer_id'].unique():#nox
        print(cid)
        tempdf = signal_nox[signal_nox['customer_id'] == cid].reset_index(drop = True)
        tempdf = tempdf.loc[~tempdf['signal_name'].str.contains('进口')].reset_index(drop = True)
        tempdf = tempdf.loc[~tempdf['signal_name'].str.contains('入口')].reset_index(drop = True)
        df_nox = pd.DataFrame()
        for i in range(len(tempdf)):
            signal_id = str(tempdf['signal_no'][i])
            device_no = str(tempdf['device_no'][i])
            dic = Hbase('172.31.235.67').getSignal('exceed', signal_id, start_hour, end_hour)
            df = pd.DataFrame.from_dict(dic,orient='index').reset_index().rename(columns={'index':'signal_no','f1:val':'no_x'})
            df['customer_id'] = cid
            df['device_no'] = device_no
            # df =  pd.merge(df, device_subfactory[["device_no","device_name",'branch_factory']],on = "device_no",how = "left")
            df_nox = df_nox.append(df)
        cems_device_nox = cems_device_nox.append(df_nox)
    cems_device_nox['no_x'] = cems_device_nox['no_x'].astype(float)
    cems_device_nox['customer_id'] = cems_device_nox['customer_id'].astype(int)
    cems_device_nox = cems_device_nox[['no_x','device_no','customer_id']].groupby(['device_no','customer_id']).mean().reset_index()
    ########################################################tdc#####################################################################################
    for cid in signal_tdc['customer_id'].unique():#tdc
        print(cid)
        tempdf = signal_tdc[signal_tdc['customer_id'] == cid].reset_index(drop = True)
        tempdf = tempdf.loc[~tempdf['signal_name'].str.contains('进口')].reset_index(drop = True)
        tempdf = tempdf.loc[~tempdf['signal_name'].str.contains('入口')].reset_index(drop = True)
        df_tdc = pd.DataFrame()
        for i in range(len(tempdf)):
            signal_id = str(tempdf['signal_no'][i])
            device_no = str(tempdf['device_no'][i])
            dic = Hbase('172.31.235.67').getSignal('exceed', signal_id, start_hour, end_hour)
            df = pd.DataFrame.from_dict(dic,orient='index').reset_index().rename(columns={'index':'signal_no','f1:val':'tdc'})
            df['customer_id'] = cid
            df['device_no'] = device_no
            # df =  pd.merge(df, device_subfactory[["device_no","device_name",'branch_factory']],on = "device_no",how = "left")
            df_tdc = df_tdc.append(df)
        cems_device_tdc = cems_device_tdc.append(df_tdc) 
    cems_device_tdc['tdc'] = cems_device_tdc['tdc'].astype(float)
    cems_device_tdc['customer_id'] = cems_device_tdc['customer_id'].astype(int)
    cems_device_tdc = cems_device_tdc[['tdc','device_no','customer_id']].groupby(['device_no','customer_id']).mean().reset_index()
    #####################################################gasflow########################################################################################
    for cid in signal_gasflow['customer_id'].unique():#gasflow
        print(cid)
        tempdf = signal_gasflow[signal_gasflow['customer_id'] == cid].reset_index(drop = True)
        tempdf = tempdf.loc[~tempdf['signal_name'].str.contains('进口')].reset_index(drop = True)
        tempdf = tempdf.loc[~tempdf['signal_name'].str.contains('入口')].reset_index(drop = True)
        df_gasflow = pd.DataFrame()
        for i in range(len(tempdf)):
            signal_id = str(tempdf['signal_no'][i])
            device_no = str(tempdf['device_no'][i])
            dic = Hbase('172.31.235.67').getSignal('exceed', signal_id, start_hour, end_hour)
            df = pd.DataFrame.from_dict(dic,orient='index').reset_index().rename(columns={'index':'signal_no','f1:val':'gasflow'})
            df['customer_id'] = cid
            df['device_no'] = device_no
            # df =  pd.merge(df, device_subfactory[["device_no","device_name",'branch_factory']],on = "device_no",how = "left")
            df_gasflow = df_gasflow.append(df)
        cems_device_gasflow = cems_device_gasflow.append(df_gasflow) 
    cems_device_gasflow['gasflow'] = cems_device_gasflow['gasflow'].astype(float)
    cems_device_gasflow['customer_id'] = cems_device_gasflow['customer_id'].astype(int)
    cems_device_gasflow = cems_device_gasflow[['gasflow','device_no','customer_id']].groupby(['device_no','customer_id']).mean().reset_index()



###################################################################################################################################################
#####################################################计算按设备计算so2########################################################################################
    cems_so2_device = pd.merge(cems_device_so2,cems_device_gasflow[['gasflow','device_no']],on = 'device_no',how = 'left')
    cems_so2_device['so2_emission'] = round(cems_so2_device['so_2']*cems_so2_device['gasflow']/1000,4)
    cems_so2_device = cems_so2_device.fillna(0)
    cems_so2_device = pd.merge(cems_so2_device,device_subfactory[['device_no','device_name']],on = 'device_no',how = 'left')
    #####################################################计算按分厂计算so2########################################################################################
    cems_so2_subfactory = pd.merge(cems_device_so2,device_subfactory[['device_no','branch_factory']],on = 'device_no',how = 'left')
    cems_so2_subfactory = pd.merge(cems_so2_subfactory,cems_device_gasflow[['gasflow','device_no']],on = 'device_no',how = 'left')
    cems_so2_subfactory['so2_emission'] = round(cems_so2_subfactory['so_2']*cems_so2_subfactory['gasflow']/1000,4)
    cems_so2_subfactory = cems_so2_subfactory[['so2_emission','customer_id','branch_factory']].groupby(['customer_id','branch_factory']).sum().reset_index()
    #####################################################计算按总厂计算so2########################################################################################
    df_2 = pd.merge(cems_device_so2,cems_device_gasflow[['gasflow','device_no']],on = 'device_no',how = 'left')
    df_2['so2_emission'] = round(df_2['so_2']*df_2['gasflow']/1000,4)
    cems_so2_factory = df_2[['so2_emission','customer_id']].groupby(['customer_id']).sum().reset_index()
    
    
    #####################################################计算按设备计算nox########################################################################################
    cems_nox_device = pd.merge(cems_device_nox,cems_device_gasflow[['gasflow','device_no']],on = 'device_no',how = 'left')
    cems_nox_device['nox_emission'] = round(cems_nox_device['no_x']*cems_nox_device['gasflow']/1000,4)
    cems_nox_device = cems_nox_device.fillna(0)
    cems_nox_device = pd.merge(cems_nox_device,device_subfactory[['device_no','device_name']],on = 'device_no',how = 'left')
    #####################################################计算按分厂计算nox########################################################################################
    cems_nox_subfactory = pd.merge(cems_device_nox,device_subfactory[['device_no','branch_factory']],on = 'device_no',how = 'left')
    cems_nox_subfactory = pd.merge(cems_nox_subfactory,cems_device_gasflow[['gasflow','device_no']],on = 'device_no',how = 'left')
    cems_nox_subfactory['nox_emission'] = round(cems_nox_subfactory['no_x']*cems_nox_subfactory['gasflow']/1000,4)
    cems_nox_subfactory = cems_nox_subfactory[['nox_emission','customer_id','branch_factory']].groupby(['customer_id','branch_factory']).sum().reset_index()
    #####################################################计算按总厂计算nox########################################################################################
    df_3 = pd.merge(cems_device_nox,cems_device_gasflow[['gasflow','device_no']],on = 'device_no',how = 'left')
    df_3['nox_emission'] = round(df_3['no_x']*df_3['gasflow']/1000,4)
    cems_nox_factory = df_3[['nox_emission','customer_id']].groupby(['customer_id']).sum().reset_index()
    
    #####################################################计算按设备计算tdc########################################################################################
    cems_tdc_device = pd.merge(cems_device_tdc,cems_device_gasflow[['gasflow','device_no']],on = 'device_no',how = 'left')
    cems_tdc_device['tdc_emission'] = round(cems_tdc_device['tdc']*cems_tdc_device['gasflow']/1000,4)
    cems_tdc_device = cems_tdc_device.fillna(0)
    cems_tdc_device = pd.merge(cems_tdc_device,device_subfactory[['device_no','device_name']],on = 'device_no',how = 'left')
    #####################################################计算按分厂计算tdc########################################################################################
    cems_tdc_subfactory = pd.merge(cems_device_tdc,device_subfactory[['device_no','branch_factory']],on = 'device_no',how = 'left')
    cems_tdc_subfactory = pd.merge(cems_tdc_subfactory,cems_device_gasflow[['gasflow','device_no']],on = 'device_no',how = 'left')
    cems_tdc_subfactory['tdc_emission'] = round(cems_tdc_subfactory['tdc']*cems_tdc_subfactory['gasflow']/1000,4)
    cems_tdc_subfactory = cems_tdc_subfactory[['tdc_emission','customer_id','branch_factory']].groupby(['customer_id','branch_factory']).sum().reset_index()
    #####################################################计算按总厂计算tdc########################################################################################
    df_4 = pd.merge(cems_device_tdc,cems_device_gasflow[['gasflow','device_no']],on = 'device_no',how = 'left')
    df_4['tdc_emission'] = round(df_4['tdc']*df_4['gasflow']/1000,4)
    cems_tdc_factory = df_4[['tdc_emission','customer_id']].groupby(['customer_id']).sum().reset_index()
    #####################################################合并设备纬度########################################################################################

    cems_by_device = pd.merge(cems_so2_device[['device_no','so2_emission']],cems_nox_device[['device_no','nox_emission']],on='device_no',how='outer')
    cems_by_device = pd.merge(cems_by_device,cems_tdc_device[['device_no','tdc_emission']],on='device_no',how='outer')
    cems_by_device = pd.merge(cems_by_device,device_subfactory[['device_no','customer_id']],on='device_no',how='left')
    #####################################################合并分厂纬度########################################################################################
    cems_by_subfactory = pd.merge(cems_so2_subfactory[['branch_factory','so2_emission','customer_id']],cems_nox_subfactory[['branch_factory','nox_emission','customer_id']],on='branch_factory',how='outer')
    cems_by_subfactory = pd.merge(cems_by_subfactory,cems_tdc_subfactory[['branch_factory','tdc_emission','customer_id']],on='branch_factory',how='outer')
    cems_by_subfactory['customer_id'] = cems_by_subfactory['customer_id'].fillna(cems_by_subfactory['customer_id_x']).fillna(cems_by_subfactory['customer_id_y'])
    cems_by_subfactory = cems_by_subfactory[['branch_factory','so2_emission','nox_emission','tdc_emission','customer_id']]
#####################################################合并总厂纬度########################################################################################
    cems_by_factory = pd.merge(cems_so2_factory, cems_nox_factory,on='customer_id',how='outer')
    cems_by_factory = pd.merge(cems_by_factory,cems_tdc_factory,on='customer_id',how='outer')
    global cems_by_device_dict,cems_by_subfactory_dict,cems_by_factory_dict
    cems_by_device_dict = cems_by_device.to_dict('record')
    print("calculation finished!")
    cems_json_by_device = json.dumps(cems_by_device_dict)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    #创建队列。有就不管，没有就自动创建
    channel.queue_declare(queue='cems_cal_by_device')
    #使用默认的交换机发送消息。exchange为空就使用默认的
    channel.basic_publish(exchange='', routing_key='cems_cal_by_device', body=cems_json_by_device)
    print(" [x] Sent Sucessfully")
    connection.close()
    
    cems_by_subfactory_dict = cems_by_subfactory.to_dict('record')
    print("calculation finished!")
    cems_json_by_subfactory = json.dumps(cems_by_subfactory_dict)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    #创建队列。有就不管，没有就自动创建
    channel.queue_declare(queue='cems_cal_by_subfactory')
    #使用默认的交换机发送消息。exchange为空就使用默认的
    channel.basic_publish(exchange='', routing_key='cems_cal_by_subfactory', body=cems_json_by_subfactory)
    print(" [x] Sent Sucessfully")
    connection.close()
    
    cems_by_factory_dict = cems_by_factory.to_dict('record')
    print("calculation finished!")
    cems_json_by_factory = json.dumps(cems_by_factory_dict)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    #创建队列。有就不管，没有就自动创建
    channel.queue_declare(queue='cems_cal_by_factory')
    #使用默认的交换机发送消息。exchange为空就使用默认的
    channel.basic_publish(exchange='', routing_key='cems_cal_by_factory', body=cems_json_by_factory)
    print(" [x] Sent Sucessfully")
    connection.close()
    
    # return cems_by_device_dict,cems_by_subfactory_dict,cems_by_factory_dict
# cems_by_device_dict,cems_by_subfactory_dict,cems_by_factory_dict = generate_data()


def task(): 
    scheduler = APScheduler()
    scheduler.api_enabled = True
    scheduler.init_app(app)
    # 定时任务，每个小时整点运行hour='*'
    scheduler.add_job(func=generate_data, trigger='cron',hour = '*' , id='cmesCal_job_1')
    scheduler.start()


# 写在main里面，IIS不会运行
task()

if __name__ == "__main__":  
    app.run(debug=True)



























