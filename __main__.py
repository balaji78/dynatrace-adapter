import requests
import json
from datacontext import DataContext
from datetime import datetime, timedelta
from config import Config
from dateutility import DateUtil
import logging
import logging.config
from dynatraceadapter import DynaTraceAdapter

#read config file
config = Config()

run_interval = config.config['schedule']['fetchinterval']
last_run =  config.config['schedule']['lastrun']
resolution = config.config['schedule']['resolution']
schedule = config.config['schedule']
int_run_interval = int("".join(filter(str.isdigit,run_interval))) 

dataContext = DataContext()

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True
})


logging.basicConfig(filename='./application.log',
                            filemode='a',
                            format='%(asctime)s, %(name)s %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG)

logging.getLogger("Dataadapter")

connection_established = False

try:
    dataContext.open()    
    connection_established = True
except Exception as e:
    logging.error(e)

start_time = DateUtil.localdate_to_utc(last_run)

#debug      
# DynaTraceAdapter().read_data("2021-07-24T04:20:00","2021-07-24T05:30:00","1m")    

dynaTraceAdapter = DynaTraceAdapter()    

if connection_established is True:
    

    str_currenttime = DateUtil.date_to_string(
                datetime.utcnow().replace(second=0, microsecond=0))
    
    dt_localtime = datetime.now().replace(second=0, microsecond=0)
    dt_startlocaltime = DateUtil.string_to_date(last_run)
    
    str_starttime = DateUtil.floor_datetime(
        start_time, int_run_interval)# from last run
    
    str_endtime = DateUtil.floor_datetime(
        str_currenttime, int_run_interval)# current time
    
    dt_starttime = DateUtil.string_to_date(str_starttime)
    dt_endtime = DateUtil.string_to_date(
        str_endtime)  # overall end time
    
    if (dt_startlocaltime + timedelta(minutes=int_run_interval)) <= dt_localtime:

        while(dt_startlocaltime <= dt_localtime):    
            str_starttime = DateUtil.date_to_string(
                dt_starttime-timedelta(minutes=1)  )
            str_endinterval = DateUtil.date_to_string(dt_starttime + timedelta(minutes=int_run_interval))
            
            dt_starttime = dt_starttime + timedelta(minutes=int_run_interval)
            dt_startlocaltime = DateUtil.datetime_from_utc_to_local(DateUtil.date_to_string(dt_starttime+ timedelta(minutes=int_run_interval)).replace("T"," "))
            #debug
            #uncomment inorder to debug the code
            # if dt_starttime > debug_date :        
            #     break
            
            logging.info("start time = {st} end time = {en}".format(st=str_starttime,en=str_endinterval))
            print("start time = {st} end time = {en}".format(st=str_starttime,en=str_endinterval))
            dynaTraceAdapter.read_data(str_starttime,str_endinterval,resolution)
        
            
            #update last run
            schedule['lastrun'] = DateUtil.date_to_string(dt_startlocaltime-timedelta(minutes=int_run_interval)).replace("T"," ")
            
            config.setValue("schedule", schedule)
            dt_localtime = datetime.now().replace(second=0, microsecond=0)
            
            
            
            
            
            



