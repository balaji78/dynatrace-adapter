from config import Config
from influxdb import InfluxDBClient
import logging


class DataContext:

    connectionErrorMessage = "Influx connection could not be established"
    influxConnection = None
    config=None

    def __init__(self):
        self.config = Config().config        
        

    def open(self):
        connection_params = self.config["influxserver"]
        
        self.influxConnection = InfluxDBClient(
            host=connection_params['host'], port=connection_params['port'], username=connection_params['username'], password=connection_params['password'], database=connection_params['database'])

        self.influxConnection.switch_database(connection_params['database'])
        try:
            self.influxConnection.ping()
        except Exception as e:
            raise Exception(self.connectionErrorMessage)
            
    def close(self):
        if self.influxConnection is not None:
            self.influxConnection.close()
            self.influxConnection = None
       

    def get(self, query):
        if self.influxConnection is not None:
            points = self.influxConnection.query(query).get_points()
        else:
            raise Exception(self.connectionErrorMessage)
        return points

    def post(self, json_body):
        flag = False
        if self.influxConnection is None:
            raise Exception(self.connectionErrorMessage)
        self.influxConnection.write_points(points=json_body)
        flag = True
        return flag

    def delete(self, query):

        flag = False
        if self.influxConnection is not None:
            self.influxConnection.query(query)
            flag = True

        else:
            raise Exception(self.connectionErrorMessage)

        return flag
