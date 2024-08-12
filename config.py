import json
class Config:
    config = None
    filename = None    
    def __init__(self) :
        self.filename = open('./config.json')
        self.config = json.load(self.filename)

    def setValue(self, key, value):
        self.config[key] = value
        
        with open('./config.json', "w") as _file:
            json.dump(self.config, _file, indent=4)