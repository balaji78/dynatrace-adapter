class DataAdapter:
    dataCollector = None
    headers = {'Content-Type': 'application/json; charset=utf-8','Authorization':None}
    datacontext = None
    connectionErrorMessage = "Influx Connection not able to establish"
    metrics_url= None
    config = None
    
    def read_data(self,start_time,end_time,resolution):
        pass
    def insert_host_data(self,response,application_id,application_name,host_id,host_name,process):
        pass
    def insert_transaction_data(self,response,application,host):
        pass
    def frame_api(self,metrics,entity,entityId,starttime,endtime,resolution):
        pass