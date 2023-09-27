import sys
import os
from dotenv import load_dotenv
# sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
from flask import Flask, app, request, flash, redirect, url_for, make_response,jsonify
from werkzeug.utils import secure_filename
from flask_pymongo import PyMongo, MongoClient
# from pymongo import MongoClient
from src.processors.config_extractor.trends_process import TrendsExtractor
from src.processors.config_extractor.dailyReport_process import DailyReportExtractor
from src.processors.config_extractor.interactive_process import InteractiveExtractor
from src.processors.config_extractor.interactive_stats_process import InteractiveStatsExtractor
from src.processors.config_extractor.overview_process import OverviewExtractor
from src.processors.config_extractor.inputs_process import Inputs
from src.processors.config_extractor.configurator import Configurator
from src.processors.config_extractor.extract_config_new import ConfigExtractor
from src.processors.dd_extractor.extractor_new import DailyInsert
from src.processors.dd_processor.maindb_three import MainDB
from src.processors.dd_extractor.daily_extractor import DailyDataExtractor
from src.processors.config_extractor.run_maindb import MainDbRunner
from src.processors.config_extractor.handle_login import Login
from src.processors.config_extractor.change_password_process import Change_Password
from src.processors.config_extractor.dailyReport_issues import DailyIssueExtractor
from src.processors.config_extractor.dry_dock_process import drydock
from src.processors.config_extractor.ais_data_process import AisData
from src.processors.config_extractor.performance_process import Performance
from src.processors.config_extractor.reports_voyage_performance import ReportsVoyagePerformance
from src.processors.config_extractor.selected_period_report import SelectedPeriod
from src.processors.config_extractor.gpt_functions import run_conversation
from bson.json_util import loads
from flask_cors import cross_origin, CORS
import zipfile
import json
import pandas as pd
from threading import Thread
import datetime
import botocore
from flask_celery import make_celery
from flask_compress import Compress #version 1.13
import openai
import time



load_dotenv()

UPLOAD_FOLDER = 'D:/Internship/Repository/Arantell/new-front-end/front_end/new-front-end/src/shared/'
ALLOWED_EXTENSIONS = {'zip'}
'''
'MONGODB_SETTINGS': {
    'db': 'test_db',
    'host': 'localhost',
    'port': 27017,
    'alias': 'default'
}
'''
default_config = {
# 'MONGO_URI': 'mongodb://localhost:27017/aranti',
'MONGO_URI': os.getenv('MONGODB_ATLAS'),
'CORS_HEADERS': {
    'ACCESS_CONTROL_ALLOW_ORIGIN': '*',
    # 'ACCESS_CONTROL_ALLOW_CREDENTIALS': True
},
# 'UPLOAD_FOLDER': UPLOAD_FOLDER,
'SECRET_KEY': os.getenv('SECRET_KEY')
}

application = Flask(__name__)
# application.config.update(default_config)
# application.config.update(
#     CELERY_BROKER_URL='redis://localhost:6379/0',
#     result_backend='redis://localhost:6379/0'
# )

#EC2
application.config.update(
    CELERY_BROKER_URL='redis://localhost:6360/0',
    result_backend='redis://localhost:6360/0'
)

celery = make_celery(application)
#cmd2=celery -A application worker -l info -P eventlet
#solo pool cmd=celery -A application worker -l info -P solo
#redis ec2==redis-server --port 6360
#flower cmd==celery -A application flower --port=5555
#nohup cmd== runs any server even if cmd terminal is closed
#gunicorn3 --workers=3 application:application
#sudo pkill -f process_name


CORS(application)
Compress(application)
# client = pymongo.MongoClient("mongodb+srv://admin:<password>@serverlessinstance0.qkuhi.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))
# mongo = PyMongo(application) #testing
mongo = PyMongo(uri=os.getenv("MONGODB_ATLAS")) #serverless
# mongo = PyMongo(uri="mongodb://localhost:27017/") #localhost

@application.route('/', methods=['GET'])
@cross_origin()
def getStatus():
    return '''
        <!doctype html>
        <h1>All Fine!!!</h1>
    '''

@application.route('/login', methods=['POST'])
@cross_origin()
def handle_login():
    username = request.args['username']
    password = request.args['password']
    obj = Login(username, password)

    result = obj.check_login_credentials()

    if result == None:
        resp = make_response("Username or Password is incorrect", 400)
        return resp
    else:
        resp = make_response(result, 200)
        return resp

@application.route('/changepassword', methods=['GET', 'POST'])
@cross_origin()
def handle_change_password():
    id = request.args['id']
    new_password = request.args['newpassword']
    # ship_imo = request.args['ship_imo']

    obj = Change_Password(id, new_password)

    result = obj.insert_into_collection()

    if result:
        resp = make_response("Successfully updated your password!", 200)
        return resp
    else:
        resp = make_response("Something went wrong. Please try again later.", 400)
        return resp



@application.route('/ship_info', methods=['GET'])
@cross_origin()
def get_ship_info():
    id = request.args['id']
    configuration = Configurator(9591301)
    ship_configs = configuration.get_ship_configs()
    if id == "":
        result = configuration.get_dict_for_ships("")
    else:
        result = configuration.get_dict_for_ships(int(id))
    # result = configuration.get_generic_group_selection()
    return {'Result': result}

@application.route('/generic_group', methods=['GET'])
@cross_origin()
def get_generic_group():
    ship_imo = request.args['ship_imo']
    configuration = Configurator(int(ship_imo))
    ship_configs = configuration.get_ship_configs()
    # result = configuration.get_dict_for_ships()
    result = configuration.get_generic_group_selection()
    groupnames = configuration.get_list_of_groupnames()

    return {'Result': result, "Groups": groupnames}

@application.route('/getSisterVessels', methods=['GET'])
@cross_origin()
def get_sister_vessel():
    ship_imo = request.args['ship_imo']
    configuration = Configurator(int(ship_imo))
    result = configuration.get_sister_vessel()
    return {'Result': result}

@application.route('/getSimilarVessels', methods=['GET'])
@cross_origin()
def get_similar_vessel():
    ship_imo = request.args['ship_imo']
    configuration = Configurator(int(ship_imo))
    result = configuration.get_similar_vessel()
    return {'Result': result}

@application.route('/getIndependentParameters', methods=['GET'])
@cross_origin()
def get_independent_parameters_list():
    ship_imo = request.args['ship_imo']
    id = request.args['id']
    configuration = Configurator(int(ship_imo))
    result = configuration.get_independent_parameters(id)
    return {'Result': result}

@application.route('/getDependentParameters', methods=['GET'])
@cross_origin()
def get_dependent_parameters_list():
    ship_imo = request.args['ship_imo']
    id = request.args['id']
    configuration = Configurator(int(ship_imo))
    result = configuration.get_dependent_parameters(id)
    return {'Result': result}

@application.route('/voyage_options', methods=['GET'])
@cross_origin()
def getVoyageOptions():
    ship_imo = request.args['ship_imo']
    configuration = Configurator(int(ship_imo))
    result = configuration.create_voyage_options_list()
    return {'Options': result}

@application.route('/trends', methods=['GET', 'POST'])
@cross_origin()
def getTrends():
    ''' Handle requests from the Trends Page'''
    args=request.args
    # formargs = request.form
    # print(args)
    ship_imo = args['ship_imo']
    group = args['group']
    duration = args['duration']
    include_outliers = args['include_outliers'] if 'include_outliers' in args else 'false'
    compare = args['compare'] if 'compare' in args else 'None'
    anomalies = args['anomalies'] if 'anomalies' in args else "true"
    noonorlogs = args['noonorlogs'] if 'noonorlogs' in args else "noon"
    indi_params = args['individual_params'] if 'individual_params' in args else ""
    # print(indi_params)
    individual_parameters=['rep_dt']
    if indi_params == "":
        individual_parameters = ['rep_dt']
    elif ',' in indi_params == False and indi_params != "":
        individual_parameters.append(indi_params)
    else:
        indi_params = indi_params.replace('[','').replace(']','').split(',')
        for i in indi_params:
            individual_parameters.append(i)
    # print("INDIVIDUAL PARAMETERS !!!!!!!", individual_parameters)
    # individual_parameters_list = individual_parameters.replace(']', '').replace('[', '').split(',')
    # print(type(individual_parameters))
    # print("INDIVIDUAL B4!!!!!!!!", individual_parameters)
    # if group == '':
    #     res = TrendsExtractor(ship_imo, group, duration, individual_parameters, include_outliers)
    #     individual_groups = res.do_steps()
    #     return {'Individual Groups': individual_groups}
    # else:
    res = TrendsExtractor(ship_imo, group, duration, individual_parameters, include_outliers, compare, anomalies, noonorlogs)
    if 'Multi Axis' in group:
        mainresult, expresult, lowerresult, upperresult, short_names_list, loaded_ballast_list, fuel_consumption, group, chart_height, variables_list = res.process_data()
        returnDict = {"mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "Short Names": short_names_list, "Ballast or Loaded": loaded_ballast_list, "Fuel Consumption": fuel_consumption, "group": group, "Chart Height": chart_height, "Variables List": variables_list}
        return returnDict
    else:
        if include_outliers == 'true':
            print("START DO STEPS")
            if 'Lastyear' in duration:
                groupList, mainresult, expresult, lowerresult, upperresult, lyexpresult, lylowerresult, lyupperresult, outlierresult, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict, ewmadict, ewmalimitdict, spe_messages_dict, subgroup_dict, outlier_messages_dict, weather_data, short_names, sisterexpected, sister_or_similar_name = res.do_steps()
                print("END DO STEPS")
                height = res.get_chart_height(groupList)
                fuelConsRes = res.process_fuel_consumption()
                # individual_params = res.individual_parameters()
                count=0
                for i in groupList:
                    if i['group_availability_code']%10 == 0:
                        count = count + 1
                # if('Multi Axis' in group):
                #     namesList = res.get_short_names_list_for_multi_axis()
                #     returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult, 'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, 'Short Names': namesList, "Chart Height": height, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict, 'EWMA': ewmadict, 'EWMA Limit': ewmalimitdict, "Issues": dict_of_issues}
                    # grpList = res.get_generic_group_list()
                variables_list = res.variable_list_according_to_order_blocks(mainresult, groupList)
                if count > 1:
                    # y_position = res.get_Y_position()
                    height_after = res.get_height_of_chart_after_double_click(groupList)
                    # y_position_after = res.get_Y_position_after_double_click()
                    returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult,
                        "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult,
                        'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, "Chart Height": height,
                        "Chart Height after Double Click": height_after, "Variables List": variables_list,
                        'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict,
                        'T2': t2dict, 'T2 Limit': t2limitdict, 'EWMA': ewmadict, 'EWMA Limit': ewmalimitdict,
                        "Short Names": short_names, "SPE Messages": spe_messages_dict, "Subgroup Dictionary": subgroup_dict,
                        "Outlier Messages": outlier_messages_dict, "Weather": weather_data, "SisterOrSimilarExpected": sisterexpected,
                        "SisterOrSimilarName": sister_or_similar_name
                    }
                else:
                    returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult,
                    "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult,
                    'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, "Chart Height": height,
                    "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict,
                    'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict, 'EWMA': ewmadict,
                    'EWMA Limit': ewmalimitdict, "Short Names": short_names, "SPE Messages": spe_messages_dict,
                    "Subgroup Dictionary": subgroup_dict, "Outlier Messages": outlier_messages_dict, "Weather": weather_data,
                    "SisterOrSimilarExpected": sisterexpected, "SisterOrSimilarName": sister_or_similar_name
                }
                    
                # print(groupList)
                # print(mainresult)
                return returnDict
            else:
                groupList, mainresult, expresult, lowerresult, upperresult, outlierresult, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict, ewmadict, ewmalimitdict, spe_messages_dict, subgroup_dict, outlier_messages_dict, weather_data, short_names, sisterexpected, sister_or_similar_name = res.do_steps()
                print("END DO STEPS")
                height = res.get_chart_height(groupList)
                fuelConsRes = res.process_fuel_consumption()
                # individual_params = res.individual_parameters()
                count=0
                for i in groupList:
                    if i['group_availability_code']%10 == 0:
                        count = count + 1
                # if('Multi Axis' in group):
                #     namesList = res.get_short_names_list_for_multi_axis()
                #     returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, 'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, 'Short Names': namesList, "Chart Height": height, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict, 'EWMA': ewmadict, 'EWMA Limit': ewmalimitdict, "Issues": dict_of_issues}
                    # grpList = res.get_generic_group_list()
                variables_list = res.variable_list_according_to_order_blocks(mainresult, groupList)
                if count > 1:
                    # y_position = res.get_Y_position()
                    height_after = res.get_height_of_chart_after_double_click(groupList)
                    # y_position_after = res.get_Y_position_after_double_click()
                    returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult,
                        "Upper": upperresult, 'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, "Chart Height": height,
                        "Chart Height after Double Click": height_after, "Variables List": variables_list,
                        'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict,
                        'T2 Limit': t2limitdict, 'EWMA': ewmadict, 'EWMA Limit': ewmalimitdict, "Short Names": short_names,
                        "SPE Messages": spe_messages_dict, "Subgroup Dictionary": subgroup_dict,
                        "Outlier Messages": outlier_messages_dict, "Weather": weather_data, "SisterOrSimilarExpected": sisterexpected,
                        "SisterOrSimilarName": sister_or_similar_name
                    }
                else:
                    returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult,
                    "Upper": upperresult, 'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, "Chart Height": height,
                    "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict,
                    'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict, 'EWMA': ewmadict,
                    'EWMA Limit': ewmalimitdict, "Short Names": short_names, "SPE Messages": spe_messages_dict,
                    "Subgroup Dictionary": subgroup_dict, "Outlier Messages": outlier_messages_dict, "Weather": weather_data,
                    "SisterOrSimilarExpected": sisterexpected, "SisterOrSimilarName": sister_or_similar_name
                }
                    
                # print(groupList)
                # print(mainresult)
                return returnDict
        else:
            if 'Lastyear' in duration:
                groupList, mainresult, expresult, lowerresult, upperresult, lyexpresult, lylowerresult, lyupperresult, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict, ewmadict, ewmalimitdict, spe_messages_dict, subgroup_dict, weather_data, short_names, sisterexpected, sister_or_similar_name = res.do_steps()
                height = res.get_chart_height(groupList)
                fuelConsRes = res.process_fuel_consumption()
                # individual_params = res.individual_parameters()
                count=0
                for i in groupList:
                    if i['group_availability_code']%10 == 0:
                        count = count + 1
                # if('Multi Axis' in group):
                #     namesList = res.get_short_names_list_for_multi_axis()
                #     returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult, "Fuel Consumption": fuelConsRes, 'Short Names': namesList, "Chart Height": height, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict, 'EWMA': ewmadict, 'EWMA Limit': ewmalimitdict, "Issues": dict_of_issues}
                    # grpList = res.get_generic_group_list()
                variables_list = res.variable_list_according_to_order_blocks(mainresult, groupList)
                if count > 1:
                    # y_position = res.get_Y_position()
                    height_after = res.get_height_of_chart_after_double_click(groupList)
                    # y_position_after = res.get_Y_position_after_double_click()
                    returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult,
                        "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult,
                        "Fuel Consumption": fuelConsRes, "Chart Height": height, "Chart Height after Double Click": height_after,
                        "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict,
                        'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict, 'EWMA': ewmadict,
                        'EWMA Limit': ewmalimitdict, "Short Names": short_names, "SPE Messages": spe_messages_dict,
                        "Subgroup Dictionary": subgroup_dict, "Weather": weather_data, "SisterOrSimilarExpected": sisterexpected,
                        "SisterOrSimilarName": sister_or_similar_name
                    }
                else:
                    returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult,
                        "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult,
                        "Fuel Consumption": fuelConsRes, "Chart Height": height, "Variables List": variables_list,
                        'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict,
                        'T2 Limit': t2limitdict, 'EWMA': ewmadict, 'EWMA Limit': ewmalimitdict, "Short Names": short_names,
                        "SPE Messages": spe_messages_dict, "Subgroup Dictionary": subgroup_dict, "Weather": weather_data,
                        "SisterOrSimilarExpected": sisterexpected, "SisterOrSimilarName": sister_or_similar_name
                }
                    
                # print(groupList)
                # print(mainresult)
                return returnDict
            else:
                groupList, mainresult, expresult, lowerresult, upperresult, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict, ewmadict, ewmalimitdict, spe_messages_dict, subgroup_dict, weather_data, short_names, sisterexpected, sister_or_similar_name = res.do_steps()
                height = res.get_chart_height(groupList)
                fuelConsRes = res.process_fuel_consumption()
                # individual_params = res.individual_parameters()
                count=0
                for i in groupList:
                    if i['group_availability_code']%10 == 0:
                        count = count + 1
                # if('Multi Axis' in group):
                #     namesList = res.get_short_names_list_for_multi_axis()
                #     returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "Fuel Consumption": fuelConsRes, 'Short Names': namesList, "Chart Height": height, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict, 'EWMA': ewmadict, 'EWMA Limit': ewmalimitdict, "Issues": dict_of_issues}
                    # grpList = res.get_generic_group_list()
                variables_list = res.variable_list_according_to_order_blocks(mainresult, groupList)
                if count > 1:
                    # y_position = res.get_Y_position()
                    height_after = res.get_height_of_chart_after_double_click(groupList)
                    # y_position_after = res.get_Y_position_after_double_click()
                    returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult,
                        "Upper": upperresult, "Fuel Consumption": fuelConsRes, "Chart Height": height,
                        "Chart Height after Double Click": height_after, "Variables List": variables_list,
                        'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict,
                        'T2': t2dict, 'T2 Limit': t2limitdict, 'EWMA': ewmadict, 'EWMA Limit': ewmalimitdict,
                        "Short Names": short_names, "SPE Messages": spe_messages_dict, "Subgroup Dictionary": subgroup_dict,
                        "Weather": weather_data, "SisterOrSimilarExpected": sisterexpected, "SisterOrSimilarName": sister_or_similar_name
                    }
                else:
                    returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, 
                        "Upper": upperresult, "Fuel Consumption": fuelConsRes, "Chart Height": height,
                        "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict,
                        'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict, 'EWMA': ewmadict,
                        'EWMA Limit': ewmalimitdict, "Short Names": short_names, "SPE Messages": spe_messages_dict,
                        "Subgroup Dictionary": subgroup_dict, "Weather": weather_data, "SisterOrSimilarExpected": sisterexpected,
                        "SisterOrSimilarName": sister_or_similar_name
                    }
                    
                # print(groupList)
                # print(mainresult)
                return returnDict

@application.route('/trends/individual/params', methods=['GET'])
@cross_origin()
def getIndividualParameters():
    ship_imo = request.args['ship_imo']
    configuration = Configurator(int(ship_imo))
    ship_configs = configuration.get_ship_configs()
    options = configuration.get_individual_parameters(equip=False, index=False)
    return {'Individual': options}

@application.route('/trends/individual/equip', methods=['GET'])
@cross_origin()
def getIndividualEquipments():
    ship_imo = request.args['ship_imo']
    configuration = Configurator(int(ship_imo))
    ship_configs = configuration.get_ship_configs()
    options = configuration.get_individual_parameters(equip=True, index=False)
    return {'Individual': options}

@application.route('/trends/individual/index', methods=['GET'])
@cross_origin()
def getIndividualIndex():
    ship_imo = request.args['ship_imo']
    configuration = Configurator(int(ship_imo))
    ship_configs = configuration.get_ship_configs()
    options = configuration.get_individual_parameters(equip=False, index=True)
    return {'Individual': options}

@application.route('/trends/table', methods=['GET', 'POST'])
@cross_origin()
def getTableGroupSelection():
    args=request.args
    # formargs = request.form
    print(args)
    ship_imo = args['ship_imo']
    group = args['group']
    duration = args['duration']
    include_outliers = args['include_outliers'] if 'include_outliers' in args else 'false'
    compare = args['compare'] if 'compare' in args else 'None'
    anomalies = args['anomalies'] if 'anomalies' in args else "true"
    noonorlogs = args['noonorlogs'] if 'noonorlogs' in args else "noon"
    indi_params = args['individual_params'] if 'individual_params' in args else ""
    print(indi_params)
    individual_parameters=['rep_dt']
    if indi_params == "":
        individual_parameters = ['rep_dt']
    elif ',' in indi_params == False and indi_params != "":
        individual_parameters.append(indi_params)
    else:
        indi_params = indi_params.replace('[','').replace(']','').split(',')
        for i in indi_params:
            individual_parameters.append(i)
    res = TrendsExtractor(ship_imo, group, duration, individual_parameters, include_outliers, compare, anomalies, noonorlogs)
    groupsList = res.get_groups_selection_for_table_component()
    returnDict = {"Groups": groupsList}
    return returnDict

@application.route('/trends/intervention', methods=['GET'])
@cross_origin()
def getTrendsIntervention():
    args = request.args
    print("ARGS", args)
    ship_imo = args['ship_imo']
    dry_dock_period = args['dry_dock_period']
    evaluation_period = args['evaluation_period']
    performance_type = args['performance_type']

    eval_data, short_names, group, chart_height, ref_data_period, eval_data_period, order_of_loss_avg, shapes, subgroup_names = drydock(dry_dock_period, evaluation_period, ship_imo, performance_type)

    return {"Evaluation Data": eval_data, "Short Names": short_names, "Group": group, "Height": chart_height, 'Trigger Period': ref_data_period, 'Effect Period': eval_data_period,
            "Loss Avg Order": order_of_loss_avg, "Shapes": shapes, 'Subgroup Names': subgroup_names}

@application.route('/dailyreport', methods=['GET'])
@cross_origin()
def getDailyReport():
    args = request.args
    print("argsss",args)
    ship_imo = args['ship_imo']
    id = args['id']
    res = DailyReportExtractor(ship_imo, id)
    category_list, category_data, column_headers, subcategory_data, dateList, latestRes, anomalyList, static_data, charter_party_values, charter_party_prediction_values, compliance_messages, listOfVesselParticulars, issuesCount,latest_vti, latest_avg_vti, source = res.process_data()
    
    return {"Category List": category_list, "Category Dictionary": category_data, 'Sub Category Dictionary': subcategory_data, 'Dates': dateList, "Latest Result": latestRes, 'Daily Report Column Headers': column_headers, 'Anomaly List': anomalyList, 'Static Data': static_data, 'Charter Party Values': charter_party_values, 'Charter Party Prediction Values': charter_party_prediction_values, "Compliance Messages": compliance_messages, "List of Vessel Particulars": listOfVesselParticulars, "Issues Count": issuesCount}

@application.route('/dailyreport/historical', methods=['GET'])
@cross_origin()
def getSpecificDailyReport():
    args = request.args
    ship_imo = args['ship_imo']
    dateString = args['dateString']
    id = args['id']
    res = DailyReportExtractor(ship_imo, id, dateString)

    category_list, category_data, column_headers, subcategory_data, dateList, latestRes, anomalyList, static_data, charter_party_values, charter_party_prediction_values, compliance_messages, listOfVesselParticulars, issuesCount, latest_vti, latest_avg_vti, source = res.process_data()
    # historicalresult = res.read_data_for_specific_date(dateString)
    print(category_data)
    print(category_list)
    print("column_headersdailydata",column_headers)
    return {"Category List": category_list, "Category Dictionary": category_data, 'Sub Category Dictionary': subcategory_data, 'Dates': dateList, "Latest Result": latestRes, 'Daily Report Column Headers': column_headers, 'Anomaly List': anomalyList, 'Static Data': static_data, 'Charter Party Values': charter_party_values, 'Charter Party Prediction Values': charter_party_prediction_values, "Compliance Messages": compliance_messages, "List of Vessel Particulars": listOfVesselParticulars, "Issues Count": issuesCount, "latest_vti":latest_vti,"latest_avg_vti":latest_avg_vti,"source":source}

@application.route('/dailyissues', methods=['GET'])
@cross_origin()
def getDailyIssues():
    args= request.args
    ship_imo = args['ship_imo']
    dateString = args['dateString'] if 'dateString' in args else ""
    id = args['id']
    res = DailyIssueExtractor(ship_imo, id, dateString)

    category_list, category_data, column_headers, subcategory_data, dateList, latestRes, anomalyList, static_data, charter_party_values, charter_party_prediction_values, compliance_messages, listOfVesselParticulars, issuesCount = res.process_data()
    # historicalresult = res.read_data_for_specific_date(dateString)
    print(category_data)
    print(category_list)
    print("column_headersdailyissuess",column_headers)
    return {"Category List": category_list, "Category Dictionary": category_data, 'Sub Category Dictionary': subcategory_data, 'Dates': dateList, "Latest Result": latestRes, 'Daily Report Column Headers': column_headers, 'Anomaly List': anomalyList, 'Static Data': static_data, 'Charter Party Values': charter_party_values, 'Charter Party Prediction Values': charter_party_prediction_values, "Compliance Messages": compliance_messages, "List of Vessel Particulars": listOfVesselParticulars, "Issues Count": issuesCount}


@application.route('/ais_data', methods=['GET'])
@cross_origin()
def getAisData():
    args = request.args
    ship_imo = args['ship_imo']

    res = AisData(ship_imo)

    valueList, ais_params, weather_params = res.process_data()
    return {'Ais': valueList, 'Ais List': ais_params, 'Weather List': weather_params}


@application.route('/interactive', methods=['GET'])
@cross_origin()
def getInteractive():
    args = request.args
    ship_imo = args['ship_imo']
    X = args['X']
    Y = args['Y']
    duration = args['duration']
    compare = args['compare'] if 'compare' in args else "None"
    stringConstantNames = args['constantNames']
    stringConstantValues = args['constantValues']
    listConstantNames = stringConstantNames.replace(']', '').replace('[', '').split(',')
    listConstantValues = stringConstantValues.replace(']', '').replace('[', '').split(',')
    values = [float(i) for i in listConstantValues]
    other_X = {}
    for i in range(len(listConstantNames)):
        if listConstantNames[i] != "None":
            other_X[listConstantNames[i]] = values[i]
        else:
            continue
    typeofinput = args['typeofinput'] if 'typeofinput' in args else "input"
    load = args['load']
    # print(other_X)
    if args.get('Z') != None:
        Z = args['Z']
        # res = InteractiveExtractor(ship_imo, X, Y, duration, Z, typeofinput, **other_X)
        res = InteractiveExtractor(ship_imo=ship_imo, X=X, Y=Y, duration=duration, load=load, compare=compare, Z=Z, typeofinput=typeofinput, **other_X)
        if typeofinput == 'input':
            try:
                x_name, y_name, z_name, result, other_dict = res.read_data()
                return {"X_name": x_name, "Y_name": y_name, "Z_name": z_name, "Result": result, "Other Dict": other_dict}
            except ValueError:
                result = res.read_data()
                resp = make_response(result, 400)
                return resp
            # x_name, y_name, z_name, result = res.read_surface_data()
        else:
            try:
                x_name, y_name, z_name, result, other_dict = res.read_data()
                return {"X_name": x_name, "Y_name": y_name, "Z_name": z_name, "Result": result, "Other Dict": other_dict}
            except ValueError:
                result = res.read_data()
                resp = make_response(result, 400)
                return resp
    else:
        res = InteractiveExtractor(ship_imo=ship_imo, X=X, Y=Y, duration=duration, load=load, compare=compare, typeofinput=typeofinput, **other_X)
        try:
            x_name, y_name, result, other_dict = res.read_data()
            return {"X_name": x_name, "Y_name": y_name, "Result": result, "Other Dict": other_dict}
        except ValueError:
            result = res.read_data()
            resp = make_response(result, 400)
            return resp

@application.route('/interactive/stats', methods=['GET'])
@cross_origin()
def getInteractiveStats():
    args = request.args
    ship_imo = args['ship_imo']
    duration = args['duration']
    stringConstantNames = args['constantNames']
    listConstantNames = stringConstantNames.replace(']', '').replace('[', '').split(',')
    res = InteractiveStatsExtractor(ship_imo, duration, listConstantNames)

    stats = res.read_data_for_stats()

    return { "Stats": stats }

@application.route('/overview', methods=['GET'])
@cross_origin()
def getOverview():
    id = request.args['id']
    if id == "":
        res = OverviewExtractor("")
    else:
        res = OverviewExtractor(int(id))
    result, actual_active_ships, total_ships, daily_data_received, total_issues = res.process_data()
    return {'Result': result, 'Active Ships': actual_active_ships, 'Total Ships': total_ships, 'Daily Data Received': daily_data_received, 'Total Issues': total_issues}

@application.route('/dailyinput', methods=['GET'])
@cross_origin()
def getDailyInput():
    return {}


@application.route('/checkprint', methods=['GET'])
@cross_origin()
def getPrint():
    res = TrendsExtractor(9591301, 'BASIC', '30Days')
    res.connect()
    result = res.process_fuel_consumption()
    return {"result": result}


def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@application.route('/sendFileToBackend', methods=['GET','POST'])
@cross_origin()
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        print("NAME",file.filename)
        if file.filename == '':
            flash('No file selected')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            # file.save(os.path.join(application.config['UPLOAD_FOLDER'], filename))
            # archive = zipfile.ZipFile(file, 'r')
            # print("ARCHIVE", archive.filename)
            # filedata = archive.open(file.filename, 'r')
            with zipfile.ZipFile(file) as archive:
                with archive.open(file.filename.replace('Zip','').replace('.zip','.xlsx')) as myfile:

                    config_extractor = ConfigExtractor(9591301, myfile.read(), True)
            config_extractor.do_steps()
            print("file", filename)
            return {'YES FILENAME': filename}
        # return {'Filename': file.filename}
    return '''
        <!doctype html>
        <title>Upload new File</title>
        <h1>Upload new File</h1>
        <form method=post enctype=multipart/form-data>
        <input type=file name=file>
        <input type=submit value=Upload>
        </form>
    '''

@application.route('/sendToMainDb', methods=['GET','POST'])
@cross_origin()
def upload_file_to_main_db():
    args = request.args
    ship_imo = args['ship_imo'] if 'ship_imo' in args else ""
    if request.method == 'POST':
        if 'file1' not in request.files or 'file2' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file1 = request.files['file1']
        file2 = request.files['file2']
        print("NAME",file1.filename, file2.filename)
        if file1.filename == '' or file2.filename == '':
            flash('One or more files not selected')
            return redirect(request.url)
        if file1 and allowed_file(file1.filename) and file2 and allowed_file(file2.filename):
            filename1 = secure_filename(file1.filename)
            filename2 = secure_filename(file2.filename)
            # file.save(os.path.join(application.config['UPLOAD_FOLDER'], filename))
            # archive = zipfile.ZipFile(file, 'r')
            # print("ARCHIVE", archive.filename)
            # filedata = archive.open(file.filename, 'r')
            # with zipfile.ZipFile(file) as archive:
            #     with archive.open(file.filename.replace('Zip','').replace('.zip','.xlsx')) as myfile:

            #         config_extractor = ConfigExtractor(9591301, myfile.read(), True)
            # config_extractor.do_steps()
            archive1 = zipfile.ZipFile(file1)
            myfile1 = archive1.open(file1.filename.replace('Zip','').replace('.zip','.xlsx'))
            archive2 = zipfile.ZipFile(file2)
            myfile2 = archive2.open(file2.filename.replace('Zip','').replace('.zip','.xlsx'))

            dailydata_extractor = DailyInsert(myfile1,myfile2,int(ship_imo),True)
            dailydata_extractor.do_steps()

            maindb_extractor = MainDB(int(ship_imo))
            maindb_extractor.get_ship_configs()
            maindb_extractor.ad_all()
            print("MAIN DB DONE!!!!!")
            maindb_extractor.update_maindb_alldoc()
            print("OUTLIER DONE!!!!!")
            maindb_extractor.update_good_voyage()
            print("GOOD VOYAGE DONE!!!!")
            maindb_extractor.update_maindb_predictions_alldoc()
            print("PREDICTIONS DONE!!!!!")
            # maindb_extractor.update_main_fuel()
            # maindb_extractor.update_sfoc()
            # maindb_extractor.update_avg_hfo()
            maindb_extractor.update_indices()
            print("INDICES CREATION DONE!!!!!")
            maindb_extractor.universal_limit()
            print("UNIVERSAL DONE!!!!!")
            maindb_extractor.universal_indices_limits()
            print("UNIVERSAL INDICES LIMITS DONE!!!!!")
            maindb_extractor.ewma_limits()
            print("EWMA LIMITS DONE!!!!!")
            maindb_extractor.indice_ewma_limit()
            print("EWMA INDICES LIMITS DONE!!!!!")
            print("file", filename1, filename2)
            return {'YES FILENAME': filename1+filename2}
        # return {'Filename': file.filename}
    return '''
        <!doctype html>
        <title>Upload new File</title>
        <h1>Upload new File</h1>
        <form method=post enctype=multipart/form-data>
        <input type=file name=file>
        <input type=submit value=Upload>
        </form>
    '''

@celery.task(name='application.run_maindb')
def run_maindb(date_list, time_list, ship_imo):
    # background_task(imo)
    # time.sleep(60)
    if len(date_list) > 0:
        for i in range(len(date_list)):
            runner = MainDbRunner(date_list[i], ship_imo, time_list[i])
            message = runner.check_fuel_and_engine_availability_and_run()
    return "done"

@application.route('/sendBackend', methods=['POST'])
@cross_origin()
def sendBackend():
    if request.method == 'POST':
        type_of_input = request.args['type']
        subtype_of_input = request.args['subtype']
        ship_imo = request.args['ship_imo']
        if type_of_input == 'fuel':
            obj=DailyDataExtractor(request.get_json(),None,int(ship_imo),False, True) if subtype_of_input != 'logs' else DailyDataExtractor(request.get_json(),None,int(ship_imo),True, True)
            obj.connect()
            date_list, time_list = obj.dailydata_insert()
            run_maindb.delay(date_list, time_list, int(ship_imo)) if subtype_of_input != 'logs' else run_maindb.delay(date_list, time_list, int(ship_imo))
            # obj=DailyDataExtractor(request.get_json(),None,int(ship_imo),False, True) if subtype_of_input != 'logs' else DailyDataExtractor(request.get_json(),None,int(ship_imo),True, True)
            # obj.connect()
            # obj.dailydata_insert()
            # thread = Thread(target=obj.dailydata_insert)
            # thread.start()
            # thread.join()
            return "{} data Updated!".format(type_of_input)
            # obj.dailydata_insert()
            # print(msg)
        elif type_of_input == 'engine':
            obj=DailyDataExtractor(None,request.get_json(),int(ship_imo),False, True) if subtype_of_input != 'logs' else DailyDataExtractor(None,request.get_json(),int(ship_imo),True, True)
            obj.connect()
            date_list, time_list = obj.dailydata_insert()
            run_maindb.delay(date_list, time_list, int(ship_imo)) if subtype_of_input != 'logs' else run_maindb.delay(date_list, time_list, int(ship_imo))
            # obj=DailyDataExtractor(None,request.get_json(),int(ship_imo),False, True) if subtype_of_input != 'logs' else DailyDataExtractor(None,request.get_json(),int(ship_imo),True, True)
            # obj.connect()
            # obj.dailydata_insert()
            # thread = Thread(target=obj.dailydata_insert)
            # thread.start()
            # thread.join()
            return "{} room data Updated!".format(type_of_input)
            # obj.dailydata_insert()
            # print(msg)
            # data = request.get_json()
            # timestamp = data['timestamp'][0]
            # date_time = datetime.datetime.fromtimestamp(float(timestamp))
            # print(date_time)
        
        # data = request.get_json()
        # date = data['rep_dt'][0]
        # timestamp = data['timestamp'][0] if 'timestamp' in data else None
        # if return_rep_dt != None:
        #     runner = MainDbRunner(return_rep_dt, ship_imo, return_timestamp)
        #     message = runner.check_fuel_and_engine_availability_and_run()
        
            # return {'Message': message}
    # return '''
    #     <!doctype html>
    #     <title>Upload new File</title>
    #     <h1>Upload new File</h1>
    #     <form method=post enctype=multipart/form-data>
    #     <input type=file name=file>
    #     <input type=submit value=Upload>
    #     </form>
    # '''


@application.route('/getSpreadsheetFromS3', methods=['GET'])
@cross_origin()
def testSpreadsheet():
    # from openpyxl import load_workbook
    # from openpyxl.utils import get_column_letter

    # import json
    ship_imo = request.args['ship_imo']
    ship_name = request.args['ship_name']
    type_of_input = request.args['type']
    subtype_of_input = request.args['subtype']
    how_many_days = request.args['how_many_days'] if 'how_many_days' in request.args else None

    import boto3
    import io
    import pandas as pd

    aws_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = 'input-templates'

    obj = Inputs(int(ship_imo), ship_name, type_of_input, subtype_of_input, aws_id, aws_secret, bucket_name)
    json_df, spreadsheet_metadata, sheet_names = obj.getSpreadsheetFromS3() if how_many_days == None else obj.getSpreadsheetFromS3(how_many_days)
    # object_prefix = ship_name + ' - ' + str(ship_imo) + '/' + type_of_input.capitalize() + '/'
    # file_name = str(ship_imo) + subtype_of_input + type_of_input + '.xlsx'
    # object_key = ship_name + ' - ' + str(ship_imo) + '/' + subtype_of_input.capitalize() + '/' + type_of_input.capitalize() + '/' + file_name
    # print(object_key)

    # s3 = boto3.client('s3', aws_access_key_id=aws_id, aws_secret_access_key=aws_secret)
    # obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    # data = obj['Body'].read()
    # print(type(data))
    # df = pd.read_excel(io.BytesIO(data), engine='openpyxl')
    # json_df = df.to_json(orient="records")
    print("JSON DF", type(json_df))
    return {"Result": json_df, "Metadata": spreadsheet_metadata, "Names": sheet_names}

@application.route('/putSpreadsheetToS3', methods=['POST'])
@cross_origin()
def putSpreadsheet():
    aws_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = os.getenv('TEMPLATE_BUCKET')
    type_of_input = request.args['type']
    subtype_of_input = request.args['subtype']
    ship_imo = request.args['ship_imo']
    ship_name = request.args['ship_name']
    file_name = str(ship_imo) + subtype_of_input + type_of_input + '.xlsx'
    object_key = ship_name + ' - ' + str(ship_imo) + '/' + subtype_of_input.capitalize() + '/' + type_of_input.capitalize() + '/' + file_name

    # body_json = request.get_json()
    body_json = request.get_data()
    obj = Inputs(int(ship_imo), ship_name, type_of_input, subtype_of_input, aws_id, aws_secret, bucket_name)
    json_df = obj.putSpreadsheetToS3(body_json)

    # keys = list(body_json.keys())
    # wb = Workbook()
    # ws = wb.active
    # print(type(body_json))
    # # with open('users.json',encoding="utf8") as f:
    # #     json_data = json.load(f)
    
    # for i in range(len(keys)):
    #     ws.cell(row=1, column=i+1, value=keys[i])
    
    # for i in range(len(keys)):
    #     for j in range(len(body_json[keys[i]])):
    #         ws.cell(row=j+2, column=i+1, value=body_json[keys[i]][j])
    # # for i in range(len(body_json)) :
    # #     sub_obj = body_json[i]
    # #     if i == 0 :
    # #         keys = list(sub_obj.keys())
    # #         for k in range(len(keys)) :
    # #             # row or column index start from 1
    # #             ws.cell(row = (i + 1), column = (k + 1), value = keys[k])
    # #     for j in range(len(keys)) :
    # #         ws.cell(row = (i + 2), column = (j + 1), value = sub_obj[keys[j]])
    # # wb.save("new-users.xlsx")
    # # print(ws.values)
    # # file_as_binary = bytearray(str(ws.values), encoding="utf8")

    # s3 = boto3.client('s3', aws_access_key_id=aws_id, aws_secret_access_key=aws_secret)
    # # obj = s3.get_object(Bucket=bucket_name, Key='new-users.xlsx')
    # try:
    #     with NamedTemporaryFile() as tmp:
    #         filename = file_name
    #         wb.save(filename)
    #     #     json_data = json.load(f)     bytearray(str(body_json), encoding="utf8")
    #         s3.upload_file(filename, bucket_name, object_key)
    # except ClientError as e:
    #     print(e)
    #     return False
    
    return json_df


@application.route('/performance/actual_baseline_foc', methods=['GET'])
@cross_origin()
def performance_actual_baseline_foc():
    ship_imo = request.args['ship_imo']

    obj = Performance(int(ship_imo), 'actual_baseline_foc')
    baseline_values_ballast, baseline_values_loaded, fuel_cons_values_ballast, fuel_cons_values_loaded, x_axis_list_ballast, x_axis_list_loaded = obj.process_data()

    return {
        'Loaded': {
            'Baseline': baseline_values_loaded, 'Fuel Cons Values': fuel_cons_values_loaded, 'X axis': x_axis_list_loaded
        },
        'Ballast': {
            'Baseline': baseline_values_ballast, 'Fuel Cons Values': fuel_cons_values_ballast, 'X axis': x_axis_list_ballast
        }
    }

@application.route('/performance/actual_baseline_foc_speed', methods=['GET'])
@cross_origin()
def performance_actual_baseline_foc_speed():
    ship_imo = request.args['ship_imo']

    obj = Performance(ship_imo, 'actual_baseline_foc_speed')
    baseline_values_speed_ballast, baseline_values_speed_loaded, fuel_cons_values_speed_ballast, fuel_cons_values_speed_loaded, speed_values_ballast, speed_values_loaded = obj.process_data()

    return {
        'Loaded': {
            'Baseline': baseline_values_speed_loaded, 'Fuel Cons Values': fuel_cons_values_speed_loaded, 'Speed Values': speed_values_loaded
        },
        'Ballast': {
            'Baseline': baseline_values_speed_ballast, 'Fuel Cons Values': fuel_cons_values_speed_ballast, 'Speed Values': speed_values_ballast
        }
    }

@application.route('/performance/deviation', methods=['GET'])
@cross_origin()
def performance_deviation():
    ship_imo = request.args['ship_imo']

    obj = Performance(ship_imo, 'deviation')
    percentage_values_ballast, percentage_values_loaded, date_list_ballast, date_list_loaded, loaded_shapes, ballast_shapes = obj.process_data()

    return {
        'Loaded': {
            'Percentage Values': percentage_values_loaded, 'Date': date_list_loaded, 'Shape': loaded_shapes
        },
        'Ballast': {
            'Percentage Values': percentage_values_ballast, 'Date': date_list_ballast, 'Shape': ballast_shapes
        }
    }

@application.route('/performance/table', methods=['GET'])
@cross_origin()
def table_data():
    ship_imo = request.args['ship_imo']

    obj = Performance(ship_imo, 'table')
    table_loaded, table_ballast, column_headers = obj.process_data()

    return {
        'Loaded': table_loaded,
        'Ballast': table_ballast,
        'Headers': column_headers
    }

@application.route('/reports/voyage_performance', methods=['GET'])
@cross_origin()
def voyage_performance():
    ship_imo = request.args['ship_imo']
    voyage = request.args['voyage']

    obj = ReportsVoyagePerformance(ship_imo, voyage)
    result = obj.process_data()

    return {"Result": result}

@application.route('/reports/selected_period', methods=["GET"])
@cross_origin()
def selected_period():
    ship_imo = request.args['ship_imo']
    fromDate = request.args['fromDate']
    toDate = request.args['toDate']

    obj = SelectedPeriod(ship_imo, fromDate, toDate)
    outliers, operationals, spe, outlier_res, operational_res, spe_res = obj.process_data()

    return {'Outliers': outliers, 'Operational': operationals, 'SPE': spe,
            'Outlier Exp': outlier_res, 'Operational Exp': operational_res,
            'SPE Exp': spe_res
            }

@application.route('/reports/get_dates', methods=["GET"])
@cross_origin()
def get_available_dates():
    ship_imo = request.args['ship_imo']

    obj = SelectedPeriod(ship_imo, "", "")

    dates = obj.get_available_dates()

    return {'Dates': dates}


# ak codes from here
@application.route('/api/chat', methods=['POST'])
@cross_origin()
def chat():
    start_time = time.time()
    user_text = request.json['text']
    reply=run_conversation(user_text)["choices"][0]["message"]['content']
    
    end_time=time.time()
    print(end_time-start_time)
    return jsonify({"reply": reply, "graphUrl": None}) # Replace None with your graph URL if needed






if __name__ == '__main__':
    application.run(debug=True)
    # application.run(host='0.0.0.0')
