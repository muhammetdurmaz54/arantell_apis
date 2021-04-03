
import datetime

document = {
    "fault_id": 1,
    "fault_name":"Suction Filter Chocked",
    "description":"",
    "message":"",
    "agents":["index1","index2","index3","SPE1","SPE2","T21"],
    "affectable_params":["param","param","param"],
    "diagnostic_file":"diagnostic.xls",
    "posterior_file": "posterior.xls",
    "succeeding_faults":[
        {
            "succeeding_fault_id":3,
            "succeeding_fault_name":"Overboard Valve Open",
        },
        {
            "succeeding_fault_id": 5,
            "succeeding_fault_name": "Something Something",
        },
        ],
    "preceding_faults": [
        {
            "preceding_fault_id": None,
            "preceding_fault_name": None,
        }],


],
}

