"""
Sample model log - Stores information whenever model is trained.
"""
import datetime

document = {
    "name":"speedfoc",
    "ship_imo":9876543,
    "creation_date": datetime.datetime(2020, 5, 17),
    "duration":"3m",
    "model_filename":"speedfoc.pickle",
    "model_url":"aws.s3.xys",
    "training_data":100,
    "training_score":0.87,
}