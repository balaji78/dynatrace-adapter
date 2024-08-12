import json

class DataCollector:
    datacollector = None

    def __init__(self) :
        filename = open('./datacollector.json')
        self.datacollector = json.load(filename)
    
    def get_applications(self):
        return self.datacollector['applications']