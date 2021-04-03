
"""
Sample for Multiparametric model configurations. Similar for time- trending.

"""

document = [

    #Trends Models (This will be repeated to nunmber of models)
{"name":"foc",
          "training_features":["speed",'w_force',"slip","others"], #or respective training features for that model
          "type":"trends",
          "target_feature":"foc",# Or any target parameter
          "durations":["3m","6m","9m","12m","dd"],
          "confidence_alpha":0.95,
          "standardize":True,
          "polynomials":2,
          "polynomials_interaction":True,
          },
    #Interactive Models
{"name":"generic_interactive",
          "type":"interactive",
          "confidence_alpha":0.95
          "clip_edges":True,
          "standardize":True,
          "polynomials":2,
          "polynomials_interaction":True,
          },
    #Anamoly Detection PLS (SPE ,T2)
{"name":"generic_interactive",
         "training_features": ["speed", 'w_force', "slip", "others"],
         "target_features": ["foc","sfoc"],
         "durations": ["3m", "6m", "9m", "12m", "dd"],
         "n_components":3,
          "type":"pls",
          "confidence_alpha":0.95,
          "standardize":True,
          "polynomials":2,
          "polynomials_interaction":True,
          },
]


