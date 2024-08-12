from dataadapter import DataAdapter
from datacollector import DataCollector
from config import Config
import requests
import logging
import math
from datacontext import DataContext
import datetime
from dateutility import DateUtil

class DynaTraceAdapter(DataAdapter):
    
    applications = None
    
    def __init__(self):
        self.datacontext = DataContext()
        self.dataCollector = DataCollector()
        self.applications = self.dataCollector.get_applications() 
        self.config = Config()
        self.headers['Authorization'] = self.config.config['datareader']['api-token']
        self.base_url = self.config.config['datareader']['base_url']
        self.metrics_url = "/api/v2/metrics/query"
        self.process_api = "/api/v1/entity/infrastructure/processes"
        self.services_api = "/api/v1/entity/services"
        
    def read_data(self,start_time,end_time,resolution):
        
        for application in self.applications:            
            for host in application['hosts']:                
                for key in host['metrics']:
                    
                    process_url = self.base_url+""+self.process_api+"?host="+(str(host['host-id']))                    
                    #get all the process running inside the host
                    all_processes = requests.get(process_url,headers=self.headers).json()
                    #filter only the mentioned processes
                    filtered_processes = self.filter_allprocess(all_processes,host)
                    processes_services = []
                    
                    for process in filtered_processes:
                        serviceUrl=self.base_url+""+self.services_api+"?entity="
                        for serviceIndex in range(0,len(process['toRelationships']['runsOnProcessGroupInstance'])):
                            serviceUrl+=str(process['toRelationships']['runsOnProcessGroupInstance'][serviceIndex])
                            if(serviceIndex!=len(process['toRelationships']['runsOnProcessGroupInstance'])-1):
                                serviceUrl+="&entity="
                        #get services inside the filtered processes    
                        process_services =  requests.get(serviceUrl,headers=self.headers).json()   
                        services = []
                        for service in process_services:
                            services.append(service)
                        json = {
                            'processId':process['entityId'],
                                'processName':process['displayName'],
                                'services':services
                                    }
                        processes_services.append(json)
                    
                    
                    metricsArray = host['metrics'][key]
                    #frame the API for the metrics specified inside the host
                    api = self.frame_api(metricsArray,key,host,application,start_time,end_time,resolution,processes_services)
                    response = None
                    
                    if api is not None:
                        try:                                                        
                            response = requests.get(api,headers=self.headers).json()                                                     
                            self.insert_host_data(response['result'],application['app-id'],application['app-name'],host['host-id'],host['host-name'])                            
                        except Exception as e:
                            logging.error(e)
                            continue
                for process in processes_services: 
                    #frame API for transaction_kpis                    
                    api = self.frame_transaction_api(host,application,start_time,end_time,resolution,process['services'])
                    if api is not None:
                        try:
                            response = requests.get(api,headers=self.headers).json()
                            self.insert_transaction_data(response['result'],application,host)
                            api = None
                        except Exception as e:
                            print(e)
                            logging.error(e)
    
    def insert_transaction_data(self,responses,application,host):
        services = {}
        get_all_service_url = self.base_url+""+self.services_api
        service_response = requests.get(get_all_service_url,headers=self.headers).json()
        for service in service_response:
            services[service['entityId']] = service['displayName']
        try:
            for response in responses:
                measure_name = response['metricId'].replace("calc:service.","")
                
                for transaction in response['data']:
                    business_transaction = None
                    query_text = ''
                    try:
                        #if not backend_reponse just inserting empty query_text
                        if measure_name != 'backend_response_time':
                            query_text = ''
                            service_name = services[transaction['dimensions'][0]]
                            try:
                                business_transaction=""+transaction['dimensions'][1]
                            except Exception as e:
                                
                                business_transaction = ''
                            
                        else:
                            #if backend_response get the query and place it in query_text
                            query_text = transaction['dimensions'][1]
                            if query_text is None:
                                query_text = ''
                            service_name = services[transaction['dimensions'][0]]
                            business_transaction = ''
                            logging.warn(query_text)
                        
                    except Exception as e:
                        continue
                    if business_transaction is not None:
                        try:
                            measure_timestamps = transaction['timestamps']
                            measure_body = []
                            if(len(measure_timestamps) == 0):
                                print("no data for measure_name = {mn} server_name = {sn} application_name ={an} service name = {service}".format(mn=measure_name,an=application['app-name'],sn=host['host-name'],service=service_name))
                                logging.info("no data for measure_name = {mn} server_name = {sn} application_name ={an} service name = {service}".format(mn=measure_name,an=application['app-name'],sn=host['host-name'],service=service_name))
                                
                                continue
                            for index in range(0,len(measure_timestamps)):
                                #omit the last value because it is always null in live data
                                if index != len(measure_timestamps)-1:
                                    value = 0.0
                                    try:
                                        value = transaction['values'][index]                    
                                        if value is None:                                          
                                            continue                 
                                        else:                                            
                                            value = round(value,2)
                                    except Exception as e:
                                        logging.error(e)
                                        print(e)
                                    #convert the ms time stamp to utc data format
                                    str_time=str(datetime.datetime.fromtimestamp(measure_timestamps[index]/1000))
                                    utc_date = DateUtil.localdate_to_utc(str_time)
                                    
                                    measure_point = {
                                        "measurement": 'transaction_kpis',
                                        "tags": { 
                                        'application_id': application['app-id'],                   
                                        'application_name': application['app-name'],
                                        'server_id':host['host-id'],
                                        'server_name': host['host-name'],
                                        'measure_name': measure_name,
                                        'business_transaction':business_transaction,
                                        'query_text':query_text  ,
                                        'service_name':service_name                  
                                        },
                                        "time": utc_date,
                                        "fields": {
                                        'measure_value': float(value)
                                    }                    
                                    }               

                                    measure_body.append(measure_point)
                            
                            
                            try:
                                self.datacontext.open()

                                if self.datacontext.influxConnection is not None:
                                    self.datacontext.post(measure_body)
                                else:
                                    logging.error(self.connectionErrorMessage)
                            except Exception as e:
                               logging.error(e)
                               pass
                            finally:
                                self.datacontext.close()
                        except Exception as e:
                            logging.error(e)
        except Exception as e:
            logging.error(e)
            print(e)        
                              
    def insert_host_data(self,response,application_id,application_name,host_id,host_name,process=None):
        for data in response:
            measurename = data['metricId'].replace("builtin:","")
            if measurename.startswith('tech.generic') :
                measurename = measurename.replace("tech.generic.","")
                if process == '' or process is None:
                    continue
            
            measure_data = data['data']
            if(len(measure_data)>0):
                measure_data = measure_data[0]['values'] 
            measure_timestamps = data['data']
            if(len(measure_timestamps)>0):
                measure_timestamps = measure_timestamps[0]['timestamps'] 
            
            if(len(measure_data) != len(measure_timestamps) or len(measure_data) == 0 or len(measure_timestamps) == 0):
                print("no data for measure_name = {mn} server_name = {sn} application_name ={an} process name = {service}".format(mn=measurename,an=application_name,sn=host_name,service=process))
                logging.info("no data for measure_name = {mn} server_name = {sn} application_name ={an} process name = {service}".format(mn=measurename,an=application_name,sn=host_name,service=process))
                
                continue
                      
            measure_body = []
            
            for index in range(0,len(measure_timestamps)):
                #omit the last value because it is always null in live data
                if index != len(measure_timestamps)-1  :
                    
                    value = 0.0
                    try:
                        value = measure_data[index]                    
                        if value is None or math.isnan(value):
                            continue                             
                        else:
                            value = round(value,2)
                        
                    except Exception as e:
                        logging.error(e)
                        print(e)
                        
                    #convert timestamp ms to utc date
                    str_time=str(datetime.datetime.fromtimestamp(measure_timestamps[index]/1000))
                    
                    utc_date = DateUtil.localdate_to_utc(str_time)
                   
                    
                    measure_point = {
                        "measurement": 'host_metrics',
                        "tags": { 
                        'application_id': application_id,                   
                        'application_name': application_name,
                        'server_id':host_id,
                        'server_name': host_name,
                        'measure_name': measurename,
                        'process':process                    
                        },
                        "time": utc_date,
                        "fields": {
                        'measure_value': float(value)
                    }                    
                    }               

                    measure_body.append(measure_point)
            
            
            try:
                self.datacontext.open()
                
                if self.datacontext.influxConnection is not None:
                    self.datacontext.post(measure_body)   
                else:
                    logging.error(self.connectionErrorMessage)
            except Exception as e:                
                pass                
            finally:
                self.datacontext.close()
          
    def frame_api(self,metrics,entity,host,application,start_time,end_time,resolution,processes_services):
        metricsList = ""     
        #framing the metrics list   
        for metricind in range(0,len(metrics)):
            metricsList+=metrics[metricind]
            if metricind != len(metrics)-1:
                metricsList+=","
        
        api=self.base_url+""+self.metrics_url
        
        if entity.endswith("builtin:service"):   
            try:             
                #iterate the process 
                for proces in processes_services: 
                        #iterate the services inside that process              
                    for service  in proces['services']:
                        api+="?entitySelector=entityId({serviceid})&metricSelector=".format(serviceid=service['entityId'])+""+entity+"."
                        api+="({metriclist})&from={starttime}&to={endtime}&resolution={resolution}".format(metriclist=metricsList,starttime=start_time,endtime=end_time,resolution=resolution)
                        
                        response = requests.get(api,headers=self.headers).json()
                        try:
                            self.insert_host_data(response['result'],application['app-id'],application['app-name'],host['host-id'],host['host-name'],service['displayName'])
                        except Exception as e:
                            logging.error(e)
                            print(e)
                        api=self.base_url+""+self.metrics_url    
                    
                    pgi_metrics = ""
                    #get process related data for each process
                    for metricind in range(0,len(host['metrics']['builtin:tech.generic'])):
                        pgi_metrics+=host['metrics']['builtin:tech.generic'][metricind]
                        if metricind != len(host['metrics']['builtin:tech.generic'])-1:
                            pgi_metrics+=","
                    api+="?entitySelector=entityId({processid})&metricSelector=".format(processid=proces['processId'])+"builtin:tech.generic"+"."
                    api+="({metriclist})&from={starttime}&to={endtime}&resolution={resolution}".format(metriclist=pgi_metrics,starttime=start_time,endtime=end_time,resolution=resolution)
                    
                    response = requests.get(api,headers=self.headers).json()
                    try:
                        self.insert_host_data(response['result'],application['app-id'],application['app-name'],host['host-id'],host['host-name'],proces['processName'])
                    except Exception as e:
                        logging.error(e)
                        print(e) 
            except Exception as e:
                logging.error(e)
                print(e)                
                return None    
        elif entity.endswith("builtin:tech"):
            
            try:            
                #iterate the processes    
                for process in processes_services:
                    api+="?entitySelector=entityId({entityId})&metricSelector=".format(entityId=process['processId'])+""+entity+"."
                    api+="({metriclist})&from={starttime}&to={endtime}&resolution={resolution}".format(metriclist=metricsList,starttime=start_time,endtime=end_time,resolution=resolution)
                    
                    response = requests.get(api,headers=self.headers).json()
                    
                    api=self.base_url+""+self.metrics_url
                    self.insert_host_data(response['result'],application['app-id'],application['app-name'],host['host-id'],host['host-name'],process['processName'])
                    
            except Exception as e:
                logging.error(e)
                print(e)
            api=None
        elif entity.endswith("builtin:host"):           
                
            api+="?entitySelector=entityId({entityId})&metricSelector=".format(entityId=host['host-id'])+""+entity+"."
            api+="({metriclist})&from={starttime}&to={endtime}&resolution={resolution}".format(metriclist=metricsList,starttime=start_time,endtime=end_time,resolution=resolution)
            
        return api
           
    def frame_transaction_api(self,host,application,start_time,end_time,resolution,services):
        calc_metrics = host['metrics']['calc:service']
        all_metrics = ""
        #frame the metrics array
        for metricind in range(len(calc_metrics)):
            all_metrics+=calc_metrics[metricind]
            if metricind != len(calc_metrics)-1:
                all_metrics+=","
        
            
        metrics = "calc:service.({allmetrics})".format(allmetrics=all_metrics)
        allservice = ""
        for index in range(0,len(services)):            
            allservice+="\""+services[index]['entityId']+"\""
            if (index != len(services)-1):
                allservice+=","
        
        api = self.base_url+""+self.metrics_url+"?entitySelector=entityId({entityId})".format(entityId=allservice)+"&metricSelector="+metrics+"&"
        api+="from={start}&to={end}&resolution={resolution}".format(start=start_time,end=end_time,resolution=resolution)
          
        return api
  
    #filter the mentioned processes from list of all processes
    def filter_allprocess(self,all_process,host):
        filtered_process = []
        for process in all_process:
            if process['displayName'] in host['processes']:
                
                filtered_process.append(process)
        return filtered_process
        