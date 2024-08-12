from datetime import datetime, timedelta
import pytz
import time

class DateUtil:

    @staticmethod
    def date_to_string(_timestamp):
        if(type(_timestamp) != type(' ')):
            _timestamp = str(_timestamp)
            _timestamp = ""+_timestamp.replace(" ", "T")+"Z"
        return _timestamp[0:19]
    
    @staticmethod
    def localdate_to_utc(_timestamp):
        local_time = pytz.timezone('Asia/Calcutta')
        if(type(_timestamp) == type(' ')):
            naive_datetime = datetime.strptime (_timestamp, "%Y-%m-%d %H:%M:%S")
            local_datetime = local_time.localize(naive_datetime, is_dst=None)
            utc_datetime = local_datetime.astimezone(pytz.utc)
            time = utc_datetime.strftime("%Y-%m-%d %H:%M:%S").replace(" ","T")            
        return time
    
    @staticmethod
    def pd_date_to_string(_timestamp):
        if(type(_timestamp) != type(' ')):
            _timestamp = str(_timestamp)           
            
        return _timestamp[0:19].replace(" ", "T")+"Z"

    @staticmethod
    def string_to_date(_timestamp):
        if type(_timestamp) == type(' '):
            _timestamp = _timestamp.replace("'", "")
            _timestamp = _timestamp.replace("T", " ")
            _timestamp = _timestamp.replace("Z", "")
            
            return datetime.strptime(_timestamp, '%Y-%m-%d %H:%M:%S')
        return _timestamp

    @staticmethod
    def floor_datetime(str_timestamp, minutes_interval):
        dt_timestamp = DateUtil.string_to_date(str_timestamp)
        difference = dt_timestamp.minute % minutes_interval
        dt_timestamp = dt_timestamp - timedelta(minutes=difference)
        dt_timestamp = DateUtil.date_to_string(dt_timestamp)
        return dt_timestamp
    
    @staticmethod
    def dt_floor_datetime(str_timestamp, minutes_interval):
        dt_timestamp = DateUtil.string_to_date(str_timestamp)
        difference = dt_timestamp.minute % minutes_interval
        dt_timestamp = dt_timestamp - timedelta(minutes=difference)
        
        return dt_timestamp

    @staticmethod
    def datetime_from_utc_to_local(utc_datetime):
        utc_datetime = datetime.strptime (utc_datetime, "%Y-%m-%d %H:%M:%S")
        
        now_timestamp = time.time()
        offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
        
        return utc_datetime + offset

    @staticmethod
    def convert_date_to_ms(str_utc_datetime):        
        dt = DateUtil.datetime_from_utc_to_local(str_utc_datetime.replace("T"," "))          
        ms =  int(dt.timestamp() * 1000)       
        return ms
        