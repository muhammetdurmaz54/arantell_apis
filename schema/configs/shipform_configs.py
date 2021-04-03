
import datetime

document = {
    'report_name':'Lub Oil Report',
    'report_id':12,
    'parameters_extract': ['eat1','rpm'],
    'parameters_track':['rpm','speed'],
    'schedule':{
        '1':{
            'days':3,
            'track':{
                'rpm': {
                    'expected': 60,
                    'high': 70,
                    'low': 50
                },
                'speed': {
                    'expected': 60,
                    'high': 70,
                    'low': 50
                }
            }
        },
        '2':{
            'days':7,
            'track':{
                'rpm': {
                    'expected': 60,
                    'high': 70,
                    'low': 50
                },
                'speed': {
                    'expected': 60,
                    'high': 70,
                    'low': 50
                }
            }
        },
        '3': {
            'days': 15,
            'track': {
                'rpm': {
                    'expected': 60,
                    'high': 70,
                    'low': 50
                },
                'speed': {
                    'expected': 60,
                    'high': 70,
                    'low': 50
                }
            }
        },
    }
}

