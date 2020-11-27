
"""
Sample for Multiparametric model configurations. Similar for time- trending.

"""

document = {"name":"speedfoc",
          "training_features":["speed",'w_force',"slip","others"],
          "target_feature":"foc",
          "model_filename":"speedfoc.pickle",
          "durations":["3m","6m","9m","12m","dd"],
          "model_type":"abc",
          "confidence_alpha":0.95,
          "standardize":True,
          "polynomials":2,
          "polynomials_interaction":True,
          }

