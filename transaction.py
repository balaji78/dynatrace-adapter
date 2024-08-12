from datacontext import DataContext
import logging

class TransactionKpi:   
    connectionErrorMessage = "Influx connection not established"
    
    def __init__(self,measure_name,application_name,server_name) :
        self.measure_name = measure_name
        self.application_name = application_name
        self.server_name = server_name
               
        self.datacontext = DataContext()
        
    def get_data(self, start_time, end_time):
        transaction_query = "select measure_value ,business_transaction from transaction_kpis where application_name = '{application_name}' and server_name = '{server_name}' and measure_name =~ /^{measure_name}$/ and time >= '{start_time}' and time <= '{end_time}'".format( application_name=self.application_name,measure_name=self.measure_name,server_name=self.server_name,start_time=start_time, end_time=end_time)
           
        data = None
        
        try:
            self.datacontext.open()
            data = self.datacontext.get(transaction_query)
        except Exception as e:
            
            if self.datacontext.influxConnection is not None:
                self.datacontext.close()
            raise Exception(e)
        finally:
            if self.datacontext.influxConnection is not None:
                self.datacontext.close()
            
            return data

    def insert_data(self,transaction_data):
        datacontext = DataContext()
        transactionBody = []
        for data in transaction_data:
            transactiondata = {
                "measurement": 'transaction_kpis',
                "tags": {
                    'application_name': data['application_name'],
                    'server_name': data['server_name'],
                    'measure_name': data['measure_name']
                },
                "time": data['time'],
                "fields": {
                    'measure_value': data['measure_value']
                }
            }

            transactionBody.append(transactiondata)
        try:
            datacontext.open()

            if datacontext.influxConnection is not None:
                datacontext.post(transactionBody)
            else:
                raise Exception(self.connectionErrorMessage)
        except Exception as e:
            logging.error(e)
            return None
        finally:
            datacontext.close()