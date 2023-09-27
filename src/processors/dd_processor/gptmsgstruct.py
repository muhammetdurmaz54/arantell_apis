from langchain.llms import OpenAI
import openai
from langchain.chat_models import ChatOpenAI
import sys
import os
import pandas
from pymongo import MongoClient
from pandasai import PandasAI
import json
import requests
from dotenv import load_dotenv
import numpy as np
import scipy.stats as st
import random
import math
from datetime import datetime


load_dotenv()

TOKEN_TYPE = os.getenv('AUTHORIZATION_TOKEN_TYPE')
ACCESS_TOKEN = os.getenv('AUTHORIZATION_ACCESS_TOKEN')
AUTHORIZATION_STRING = TOKEN_TYPE + " " + ACCESS_TOKEN

MONGODB_URI = os.getenv('MONGODB_ATLAS')
client = MongoClient(MONGODB_URI)
# client = MongoClient("mongodb://localhost:27017/aranti")
db=client.get_database("aranti")
database = db

 
open_ai_test_key="sample"
openai.api_key=open_ai_test_key

def spe_limit_compare(ship_imo,date,parameter,duration):
    # date_to_pick=datetime.strptime(date, '%d/%m/%Y')
    date_to_pick=date.split('-')
    ship_imo=int(ship_imo)
    if duration=='6':
        duration_selected='m6'
    elif duration=='12':
        duration_selected='m12'
    elif duration=='l6':
        duration_selected='ly_m6'
    elif duration=='l12':
        duration_selected='ly_12'
    ship_configs_collection=database.get_collection("ship")
    ship_configs=ship_configs_collection.find({"ship_imo": ship_imo})[0]
    maindb_collection = database.get_collection("Main_db")
    maindb=maindb_collection.find({"ship_imo": ship_imo,"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0]
    spe_limits_db=ship_configs['spe_limits'][parameter][duration_selected]
    spe_val=maindb['processed_daily_data'][parameter]['SPEy'][duration_selected]
    if pandas.isnull(spe_limits_db)==False and pandas.isnull(spe_val)==False:
        spe_limits={}
        spe_limits[str(ship_configs['parameter_anamoly']['SPE_alpha1']['alpha'])]=round(spe_limits_db['zero_two'],4)
        spe_limits[str(ship_configs['parameter_anamoly']['SPE_alpha2']['alpha'])]=round(spe_limits_db['zero_one'],4)
        spe_limits[str(ship_configs['parameter_anamoly']['SPE_alpha3']['alpha'])]=round(spe_limits_db['zero_zero_five'],4)
        final_dict={"limits corresponding to alpha":spe_limits,"spe value":round(spe_val,4)}
        print(final_dict)
        return json.dumps(final_dict),ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected
    else:
        return "either spe value or spe limit is not available",None,None,None,None,None
    
def indice_spe_limit_compare(ship_imo,date,indice,duration):
    # date_to_pick=datetime.strptime(date, '%d/%m/%Y')
    date_to_pick=date.split('-')
    ship_imo=int(ship_imo)
    if duration=='6':
        duration_selected='m6'
    elif duration=='12':
        duration_selected='m12'
    elif duration=='l6':
        duration_selected='ly_m6'
    elif duration=='l12':
        duration_selected='ly_12'
    ship_configs_collection=database.get_collection("ship")
    ship_configs=ship_configs_collection.find({"ship_imo": ship_imo})[0]
    maindb_collection = database.get_collection("Main_db")
    maindb=maindb_collection.find({"ship_imo": ship_imo,"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0]
    spe_limits_db=ship_configs['spe_limits_indices'][indice][duration_selected]
    spe_val=maindb['independent_indices'][indice]['SPEy'][duration_selected]
    if pandas.isnull(spe_limits_db)==False and pandas.isnull(spe_val)==False:
        spe_limits={}
        spe_limits[str(ship_configs['parameter_anamoly']['SPE_alpha1']['alpha'])]=round(spe_limits_db['zero_two'],4)
        spe_limits[str(ship_configs['parameter_anamoly']['SPE_alpha2']['alpha'])]=round(spe_limits_db['zero_one'],4)
        spe_limits[str(ship_configs['parameter_anamoly']['SPE_alpha3']['alpha'])]=round(spe_limits_db['zero_zero_five'],4)
        final_dict={"limits corresponding to alpha":spe_limits,"spe value":round(spe_val,4)}
        print(final_dict)
        return json.dumps(final_dict),ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected
    else:
        return "either spe value or spe limit is not available",None,None,None,None,None

def ewma_limit_compare(ship_imo,date,parameter,duration):
    date_to_pick=date.split('-')
    ship_imo=int(ship_imo)
    if duration=='6':
        duration_selected='m6'
    elif duration=='12':
        duration_selected='m12'
    elif duration=='l6':
        duration_selected='ly_m6'
    elif duration=='l12':
        duration_selected='ly_12'
    ship_configs_collection=database.get_collection("ship")
    ship_configs=ship_configs_collection.find({"ship_imo": ship_imo})[0]
    maindb_collection = database.get_collection("Main_db")
    maindb=maindb_collection.find({"ship_imo": ship_imo,"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0]
    try:
        if len(ship_configs['ewma_limits'][parameter][duration_selected])>1 and len(maindb['processed_daily_data'][parameter]['ewma'][duration_selected])>1:
            ewma_limits_db=ship_configs['ewma_limits'][parameter][duration_selected][2]
            ewma_val=maindb['processed_daily_data'][parameter]['ewma'][duration_selected][2]
            if len(ewma_limits_db)>1 and pandas.isnull(ewma_val)==False:
                ewma_limits={}
                ewma_limits[str(ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha1']['alpha'])]=round(ewma_limits_db[0],4)
                ewma_limits[str(ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha2']['alpha'])]=round(ewma_limits_db[1],4)
                ewma_limits[str(ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha3']['alpha'])]=round(ewma_limits_db[2],4)
                final_dict={"limits corresponding to alpha":ewma_limits,"ewma value":round(ewma_val,4)}
                print(final_dict)
                return json.dumps(final_dict),ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected
            else:
                return "either ewma value or ewma limit is not available",None,None,None,None,None
        else:
            return "either ewma value or ewma limit is not available",None,None,None,None,None
    except:
        return "either ewma value or ewma limit is not available",None,None,None,None,None
    

def indice_ewma_limit_compare(ship_imo,date,indice,duration):
    date_to_pick=date.split('-')
    ship_imo=int(ship_imo)
    if duration=='6':
        duration_selected='m6'
    elif duration=='12':
        duration_selected='m12'
    elif duration=='l6':
        duration_selected='ly_m6'
    elif duration=='l12':
        duration_selected='ly_12'
    ship_configs_collection=database.get_collection("ship")
    ship_configs=ship_configs_collection.find({"ship_imo": ship_imo})[0]
    maindb_collection = database.get_collection("Main_db")
    maindb=maindb_collection.find({"ship_imo": ship_imo,"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0]
    try:
        if len(ship_configs['mewma_limits'][indice][duration_selected])>1:
            mewma_limits_db=ship_configs['mewma_limits'][indice][duration_selected]
            mewma_val=maindb['independent_indices'][indice]['mewma_val'][duration_selected]
            if len(mewma_limits_db)>1 and pandas.isnull(mewma_val)==False:
                mewma_limits={}
                mewma_limits[str(ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha1']['alpha'])]=round(mewma_limits_db[0],4)
                mewma_limits[str(ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha2']['alpha'])]=round(mewma_limits_db[1],4)
                mewma_limits[str(ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha3']['alpha'])]=round(mewma_limits_db[2],4)
                final_dict={"limits corresponding to alpha":mewma_limits,"mewma value":round(mewma_val,4)}
                print(final_dict)
                return json.dumps(final_dict),ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected
            else:
                return "either mewma value or mewma limit is not available",None,None,None,None,None
        else:
            return "either mewma value or mewma limit is not available",None,None,None,None,None
    except:
        return "either mewma value or mewma limit is not available",None,None,None,None,None
    

def t2_limit_compare(ship_imo,date,parameter,duration):
    date_to_pick=date.split('-')
    ship_imo=int(ship_imo)
    if duration=='6':
        duration_selected='m6'
    elif duration=='12':
        duration_selected='m12'
    elif duration=='l6':
        duration_selected='ly_m6'
    elif duration=='l12':
        duration_selected='ly_12'
    ship_configs_collection=database.get_collection("ship")
    ship_configs=ship_configs_collection.find({"ship_imo": ship_imo})[0]
    maindb_collection = database.get_collection("Main_db")
    maindb=maindb_collection.find({"ship_imo": ship_imo,"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0]
    t2_limits_db=ship_configs['t2_limits'][parameter]
    t2_val=maindb['processed_daily_data'][parameter]['t2_initial'][duration_selected]
    if pandas.isnull(t2_limits_db)==False and pandas.isnull(t2_val)==False:
        t2_limits={}
        t2_limits[str(ship_configs['parameter_anamoly']['T2_alpha1']['alpha'])]=round(t2_limits_db['zero_two'],4)
        t2_limits[str(ship_configs['parameter_anamoly']['T2_alpha2']['alpha'])]=round(t2_limits_db['zero_one'],4)
        t2_limits[str(ship_configs['parameter_anamoly']['T2_alpha3']['alpha'])]=round(t2_limits_db['zero_zero_five'],4)
        final_dict={"limits corresponding to alpha":t2_limits,"t2 value":round(t2_val,4)}
        print(final_dict)
        return json.dumps(final_dict),ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected
    else:
        return "either t2 value or t2 limit is not available",None,None,None,None,None


def indice_t2_limit_compare(ship_imo,date,indice,duration):
    date_to_pick=date.split('-')
    ship_imo=int(ship_imo)
    if duration=='6':
        duration_selected='m6'
    elif duration=='12':
        duration_selected='m12'
    elif duration=='l6':
        duration_selected='ly_m6'
    elif duration=='l12':
        duration_selected='ly_12'
    ship_configs_collection=database.get_collection("ship")
    ship_configs=ship_configs_collection.find({"ship_imo": ship_imo})[0]
    maindb_collection = database.get_collection("Main_db")
    maindb=maindb_collection.find({"ship_imo": ship_imo,"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0]
    t2_limits_db=ship_configs['t2_limits_indices'][indice]
    t2_val=maindb['processed_daily_data'][indice]['t2_initial'][duration_selected]
    if pandas.isnull(t2_limits_db)==False and pandas.isnull(t2_val)==False:
        t2_limits={}
        t2_limits[str(ship_configs['parameter_anamoly']['T2_alpha1']['alpha'])]=round(t2_limits_db['zero_two'],4)
        t2_limits[str(ship_configs['parameter_anamoly']['T2_alpha2']['alpha'])]=round(t2_limits_db['zero_one'],4)
        t2_limits[str(ship_configs['parameter_anamoly']['T2_alpha3']['alpha'])]=round(t2_limits_db['zero_zero_five'],4)
        final_dict={"limits corresponding to alpha":t2_limits,"t2 value":round(t2_val,4)}
        print(final_dict)
        return json.dumps(final_dict),ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected
    else:
        return "either t2 value or t2 limit is not available",None,None,None,None,None

def outlier_limit_compare(ship_imo,date,parameter,duration):
    date_to_pick=date.split('-')
    ship_imo=int(ship_imo)
    if duration=='6':
        duration_selected='m6'
    elif duration=='12':
        duration_selected='m12'
    elif duration=='l6':
        duration_selected='ly_m6'
    elif duration=='l12':
        duration_selected='ly_12'
    ship_configs_collection=database.get_collection("ship")
    ship_configs=ship_configs_collection.find({"ship_imo": ship_imo})[0]
    maindb_collection = database.get_collection("Main_db")
    maindb=maindb_collection.find({"ship_imo": ship_imo,"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0]
    outlier_limits_db=maindb['processed_daily_data'][parameter]['outlier_limit_value']
    within_outlier_limits=maindb['processed_daily_data'][parameter]['within_outlier_limits'][duration_selected]
    processed_value=maindb['processed_daily_data'][parameter]['processed']
    print(within_outlier_limits,type(within_outlier_limits))
    if pandas.isnull(within_outlier_limits)==False:
        if within_outlier_limits==True:
            final_dict={"outlier range if processed falls under outlier range it is not outlier":outlier_limits_db,"outlier mesage":"It is not outlier/the value is within outlier limits","processed_value":processed_value}
            print(final_dict)
            return json.dumps(final_dict),ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected
        elif within_outlier_limits==False:
            final_dict={"outlier range if processed falls under outlier range it is not outlier":outlier_limits_db,"outlier mesage":"It is an outlier/the value is exceeding outlier limits","processed_value":processed_value}
            print(final_dict)
            return json.dumps(final_dict),ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected

    else:
        return "either processed value or outlier limit is not available",None,None,None,None,None


def operational_limit_compare(ship_imo,date,parameter,duration):
    date_to_pick=date.split('-')
    ship_imo=int(ship_imo)
    if duration=='6':
        duration_selected='m6'
    elif duration=='12':
        duration_selected='m12'
    elif duration=='l6':
        duration_selected='ly_m6'
    elif duration=='l12':
        duration_selected='ly_12'
    ship_configs_collection=database.get_collection("ship")
    ship_configs=ship_configs_collection.find({"ship_imo": ship_imo})[0]
    maindb_collection = database.get_collection("Main_db")
    maindb=maindb_collection.find({"ship_imo": ship_imo,"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0]
    operational_limits_db=maindb['processed_daily_data'][parameter]['operational_limit_value']
    within_operational_limits=maindb['processed_daily_data'][parameter]['within_operational_limits'][duration_selected]
    processed_value=maindb['processed_daily_data'][parameter]['processed']
    print(within_operational_limits,type(within_operational_limits))
    if pandas.isnull(within_operational_limits)==False:
        if within_operational_limits==True:
            final_dict={"operational range if processed falls under operational range it is operational":operational_limits_db,"operational mesage":"It is not outlier/the value is within operational limits","processed_value":processed_value}
            print(final_dict)
            return json.dumps(final_dict),ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected
        elif within_operational_limits==False:
            final_dict={"operational range if processed falls under operational range it is operational":operational_limits_db,"operational mesage":"It is an outlier/the value is exceeding operational limits","processed_value":processed_value}
            print(final_dict)
            return json.dumps(final_dict),ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected

    else:
        return "either processed value or operational limit is not available",None,None,None,None,None
  

  
def run_conversation(query,second_query):
    messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating parameters beyond predefined thresholds that are told to you"},{"role": "user", "content": query}]
    functions = [
        # parameters here are just for howing struncture it will be changed based on function parameter decided
        {
            "name": "spe_limit_compare",
            "description": "for the parameter given, Evaluate the spe(Square Prediction Error) in reference to its limit/thresholds for the given alpha values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ship_imo": {
                        "type": "string",
                        "description": "imo of ship, e.g. 9205926,9595888",
                    },
                    "date": {
                        "type": "string",
                        "description": "date, e.g. 2016-02-15",
                    },
                    "parameter": {
                        "type": "string",
                        "description": "parameter, e.g.pwr,rpm,speed",
                    },
                    "duration": {
                        "type": "string",
                        "description": "duration, e.g.6,12,l6,l12",
                    }
                },
                "required": ["ship_imo","date","parameter","duration"],
            },
        },
        {
            "name": "indice_spe_limit_compare",
            "description": "for the indice given, Evaluate the spe(Square Prediction Error) in reference to its limit/thresholds for the given alpha values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ship_imo": {
                        "type": "string",
                        "description": "imo of ship, e.g. 9205926,9595888",
                    },
                    "date": {
                        "type": "string",
                        "description": "date, e.g. 2016-02-15",
                    },
                    "indice": {
                        "type": "string",
                        "description": "indice, e.g.main_fuel_index,performance_index,ml_12",
                    },
                    "duration": {
                        "type": "string",
                        "description": "duration, e.g.6,12,l6,l12",
                    }
                },
                "required": ["ship_imo","date","parameter","duration"],
            },
        },
        {
            "name": "ewma_limit_compare",
            "description": "for the paramter given, Evaluate ewma Exponentially weighted average of spe)  with reference to its limit/thresholds  for given alpha values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ship_imo": {
                        "type": "string",
                        "description": "imo of ship, e.g. 9205926,9595888",
                    },
                    "date": {
                        "type": "string",
                        "description": "date, e.g. 2016-02-15",
                    },
                    "parameter": {
                        "type": "string",
                        "description": "parameter, e.g.pwr,rpm,speed",
                    },
                    "duration": {
                        "type": "string",
                        "description": "duration, e.g.6,12,l6,l12",
                    }
                },
                "required": ["ship_imo","date","parameter","duration"],
            },
        },
        {
            "name": "indice_ewma_limit_compare",
            "description": "for the indice given, Evaluate ewma Exponentially weighted average of spe)  with reference to its limit/thresholds  for given alpha values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ship_imo": {
                        "type": "string",
                        "description": "imo of ship, e.g. 9205926,9595888",
                    },
                    "date": {
                        "type": "string",
                        "description": "date, e.g. 2016-02-15",
                    },
                    "indice": {
                        "type": "string",
                        "description": "indice, e.g.main_fuel_index,performance_index,ml_12",
                    },
                    "duration": {
                        "type": "string",
                        "description": "duration, e.g.6,12,l6,l12",
                    }
                },
                "required": ["ship_imo","date","parameter","duration"],
            },
        },
        {
            "name": "t2_limit_compare",
            "description": "for the parameter given, compare t2(Hotelling T squared)of the given set of parameters  with reference to its limit/thresholds  for given alpha values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ship_imo": {
                        "type": "string",
                        "description": "imo of ship, e.g. 9205926,9595888",
                    },
                    "date": {
                        "type": "string",
                        "description": "date, e.g. 2016-02-15",
                    },
                    "parameter": {
                        "type": "string",
                        "description": "parameter, e.g.pwr,rpm,speed",
                    },
                    "duration": {
                        "type": "string",
                        "description": "duration, e.g.6,12,l6,l12",
                    }
                },
                "required": ["ship_imo","date","parameter","duration"],
            },
        },
        {
            "name": "indice_t2_limit_compare",
            "description": "for the indice given, compare t2(Hotelling T squared)of the given set of parameters  with reference to its limit/thresholds  for given alpha values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "ship_imo": {
                        "type": "string",
                        "description": "imo of ship, e.g. 9205926,9595888",
                    },
                    "date": {
                        "type": "string",
                        "description": "date, e.g. 2016-02-15",
                    },
                    "indice": {
                        "type": "string",
                        "description": "indice, e.g.main_fuel_index,performance_index,ml_12",
                    },
                    "duration": {
                        "type": "string",
                        "description": "duration, e.g.6,12,l6,l12",
                    }
                },
                "required": ["ship_imo","date","parameter","duration"],
            },
        },
        {
            "name": "outlier_limit_compare",
            "description": "get outlier message",
            "parameters": {
                "type": "object",
                "properties": {
                    "ship_imo": {
                        "type": "string",
                        "description": "imo of ship, e.g. 9205926,9595888",
                    },
                    "date": {
                        "type": "string",
                        "description": "date, e.g. 2016-02-15",
                    },
                    "parameter": {
                        "type": "string",
                        "description": "parameter, e.g.pwr,rpm,speed",
                    },
                    "duration": {
                        "type": "string",
                        "description": "duration, e.g.6,12,l6,l12",
                    }
                },
                "required": ["ship_imo","date","parameter","duration"],
            },
        },
        {
            "name": "operational_limit_compare",
            "description": "get operational outlier message",
            "parameters": {
                "type": "object",
                "properties": {
                    "ship_imo": {
                        "type": "string",
                        "description": "imo of ship, e.g. 9205926,9595888",
                    },
                    "date": {
                        "type": "string",
                        "description": "date, e.g. 2016-02-15",
                    },
                    "parameter": {
                        "type": "string",
                        "description": "parameter, e.g.pwr,rpm,speed",
                    },
                    "duration": {
                        "type": "string",
                        "description": "duration, e.g.6,12,l6,l12",
                    }
                },
                "required": ["ship_imo","date","parameter","duration"],
            },
        }


    ]
    # this is our first response which decides the function to call if needed
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0613",
        messages=messages,
        functions=functions,
        function_call="auto", 
        # temperature=0,
        max_tokens = 200,
    )
    print('response',response)
    response_message = response["choices"][0]["message"]
    if response_message.get("function_call"):
        # these are all functions available
        available_functions = {
            "spe_limit_compare": spe_limit_compare,
            "indice_spe_limit_compare": indice_spe_limit_compare,
            "ewma_limit_compare":ewma_limit_compare,
            "indice_ewma_limit_compare":indice_ewma_limit_compare,
            "t2_limit_compare":t2_limit_compare,
            "indice_t2_limit_compare":indice_t2_limit_compare,
            "outlier_limit_compare":outlier_limit_compare,
            "operational_limit_compare":operational_limit_compare
        } 
        function_name = response_message["function_call"]["name"]
        fuction_to_call = available_functions[function_name]
        function_args = json.loads(response_message["function_call"]["arguments"])
        # parameters passed based on function decided by gpt responce
        if function_name=="spe_limit_compare":
            ship_imo=function_args.get("ship_imo")
            date=function_args.get("date")
            parameter=function_args.get("parameter")
            duration=function_args.get("duration")
            function_response,ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected = fuction_to_call(ship_imo,date,parameter,duration)
            print("function responseeeee",function_response)
            date_to_pick=date.split('-')
            messages.append(response_message) 
            messages.append({"role": "function","name": function_name,"content": function_response}) 
            second_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=messages,temperature=0,max_tokens = 200)
            second_query_new= "This is previous reply to the query that you gave"+" "+second_response["choices"][0]["message"]['content']+second_query
            second_messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating parameters beyond predefined thresholds that are told to you"},{"role": "user", "content": second_query_new}]
            Third_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=second_messages,temperature=0,max_tokens = 200)
            # print("second",second_response)
            if duration_selected!=None:
                for i in range(0,3):    
                    if len(maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected])>1 and pandas.isnull(maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected][i])==False:
                        if ship_configs['parameter_anamoly']['SPE_alpha1']['message'] in maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected][i]:
                            if "---" in maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected][i]:
                                gpt_split=gpt_db_message.split("---")[0]
                                gpt_db_message=gpt_split+"---"+str(Third_response["choices"][0]["message"]['content'])
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"processed_daily_data."+parameter+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)
                            else:
                                gpt_db_message=maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected][i]+"---"+str(Third_response["choices"][0]["message"]['content'])
                                gpt_split=gpt_db_message.split("---")[0]
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"processed_daily_data."+parameter+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)
                        elif ship_configs['parameter_anamoly']['SPE_alpha2']['message'] in maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected][i]:
                            if "---" in maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected][i]:
                                gpt_split=gpt_db_message.split("---")[0]
                                gpt_db_message=gpt_split+"---"+str(Third_response["choices"][0]["message"]['content'])
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"processed_daily_data."+parameter+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)
                            else:
                                gpt_db_message=maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected][i]+"---"+str(Third_response["choices"][0]["message"]['content'])
                                gpt_split=gpt_db_message.split("---")[0]
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"processed_daily_data."+parameter+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)
                        elif ship_configs['parameter_anamoly']['SPE_alpha3']['message'] in maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected][i]:
                            if "---" in maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected][i]:
                                gpt_split=gpt_db_message.split("---")[0]
                                gpt_db_message=gpt_split+"---"+str(Third_response["choices"][0]["message"]['content'])
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"processed_daily_data."+parameter+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)
                            else:
                                gpt_db_message=maindb['processed_daily_data'][parameter]['spe_messages'][duration_selected][i]+"---"+str(Third_response["choices"][0]["message"]['content'])
                                gpt_split=gpt_db_message.split("---")[0]
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"processed_daily_data."+parameter+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)

        elif function_name=="indice_spe_limit_compare":
            ship_imo=function_args.get("ship_imo")
            date=function_args.get("date")
            indice=function_args.get("indice")
            duration=function_args.get("duration")
            function_response,ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected = fuction_to_call(ship_imo,date,indice,duration)
            print("function responseeeee",function_response)
            date_to_pick=date.split('-')
            messages.append(response_message) 
            messages.append({"role": "function","name": function_name,"content": function_response}) 
            second_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=messages,temperature=0,max_tokens = 200)
            second_query_new= "This is previous reply to the query that you gave"+" "+second_response["choices"][0]["message"]['content']+second_query
            second_messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating indices beyond predefined thresholds that are told to you"},{"role": "user", "content": second_query_new}]
            Third_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=second_messages,temperature=0,max_tokens = 200)
            # print("second",second_response)
            if duration_selected!=None:
                for i in range(0,3):    
                    if len(maindb['independent_indices'][indice]['spe_messages'][duration_selected])>1 and pandas.isnull(maindb['independent_indices'][indice]['spe_messages'][duration_selected][i])==False:
                        if ship_configs['equipment_anamoly']['SPE_alpha1']['message'] in maindb['independent_indices'][indice]['spe_messages'][duration_selected][i]:
                            if "---" in maindb['independent_indices'][indice]['spe_messages'][duration_selected][i]:
                                gpt_split=gpt_db_message.split("---")[0]
                                gpt_db_message=gpt_split+"---"+str(Third_response["choices"][0]["message"]['content'])
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"independent_indices."+indice+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)
                            else:
                                gpt_db_message=maindb['independent_indices'][indice]['spe_messages'][duration_selected][i]+"---"+str(Third_response["choices"][0]["message"]['content'])
                                gpt_split=gpt_db_message.split("---")[0]
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"independent_indices."+indice+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)
                        elif ship_configs['equipment_anamoly']['SPE_alpha2']['message'] in maindb['independent_indices'][indice]['spe_messages'][duration_selected][i]:
                            if "---" in maindb['independent_indices'][indice]['spe_messages'][duration_selected][i]:
                                gpt_split=gpt_db_message.split("---")[0]
                                gpt_db_message=gpt_split+"---"+str(Third_response["choices"][0]["message"]['content'])
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"independent_indices."+indice+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)
                            else:
                                gpt_db_message=maindb['independent_indices'][indice]['spe_messages'][duration_selected][i]+"---"+str(Third_response["choices"][0]["message"]['content'])
                                gpt_split=gpt_db_message.split("---")[0]
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"independent_indices."+indice+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)
                        elif ship_configs['equipment_anamoly']['SPE_alpha3']['message'] in maindb['independent_indices'][indice]['spe_messages'][duration_selected][i]:
                            if "---" in maindb['independent_indices'][indice]['spe_messages'][duration_selected][i]:
                                gpt_split=gpt_db_message.split("---")[0]
                                gpt_db_message=gpt_split+"---"+str(Third_response["choices"][0]["message"]['content'])
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"independent_indices."+indice+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)
                            else:
                                gpt_db_message=maindb['independent_indices'][indice]['spe_messages'][duration_selected][i]+"---"+str(Third_response["choices"][0]["message"]['content'])
                                gpt_split=gpt_db_message.split("---")[0]
                                # maindb_collection.update_one(maindb_collection.find({"ship_imo": int(ship_imo),"processed_daily_data.rep_dt.processed":datetime(int(date_to_pick[0]),int(date_to_pick[1]),int(date_to_pick[2]),12)})[0],{"$set":{"independent_indices."+indice+".spe_messages."+duration_selected+"."+str(i):gpt_db_message}})
                                # print(gpt_db_message)
                                # print(gpt_split)

        elif function_name=="ewma_limit_compare":
            ship_imo=function_args.get("ship_imo")
            date=function_args.get("date")
            parameter=function_args.get("parameter")
            duration=function_args.get("duration")
            function_response,ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected = fuction_to_call(ship_imo,date,parameter,duration)
            print("function responseeeee",function_response)
            messages.append(response_message) 
            messages.append({"role": "function","name": function_name,"content": function_response}) 
            second_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=messages,temperature=0,max_tokens = 200)
            second_query_new= "This is previous reply to the query that you gave"+" "+second_response["choices"][0]["message"]['content']+second_query
            second_messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating parameters beyond predefined thresholds that are told to you"},{"role": "user", "content": second_query_new}]
            Third_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=second_messages,temperature=0,max_tokens = 200)
            print("second",second_response)
            if duration_selected!=None:
                for i in range(0,3):    
                    if pandas.isnull(maindb['processed_daily_data'][parameter]['ewma_messages'])==False and duration_selected in maindb['processed_daily_data'][parameter]['ewma_messages'] and  len(maindb['processed_daily_data'][parameter]['ewma_messages'][duration_selected])>1 and pandas.isnull(maindb['processed_daily_data'][parameter]['ewma_messages'][duration_selected][i])==False:
                        if ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha1']['message'] in maindb['processed_daily_data'][parameter]['ewma_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha1']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)
                        elif ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha2']['message'] in maindb['processed_daily_data'][parameter]['ewma_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha2']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)
                        elif ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha3']['message'] in maindb['processed_daily_data'][parameter]['ewma_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['parameter_anamoly']['MEWMA_CUMSUM_alpha3']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)

        elif function_name=="indice_ewma_limit_compare":
            ship_imo=function_args.get("ship_imo")
            date=function_args.get("date")
            indice=function_args.get("indice")
            duration=function_args.get("duration")
            function_response,ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected = fuction_to_call(ship_imo,date,indice,duration)
            print("function responseeeee",function_response)
            messages.append(response_message) 
            messages.append({"role": "function","name": function_name,"content": function_response}) 
            second_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=messages,temperature=0,max_tokens = 200)
            second_query_new= "This is previous reply to the query that you gave"+" "+second_response["choices"][0]["message"]['content']+second_query
            second_messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating indices beyond predefined thresholds that are told to you"},{"role": "user", "content": second_query_new}]
            Third_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=second_messages,temperature=0,max_tokens = 200)
            print("second",second_response)
            if duration_selected!=None:
                for i in range(0,3):    
                    if pandas.isnull(maindb['independent_indices'][indice]['mewma_messages'])==False and duration_selected in maindb['independent_indices'][indice]['mewma_messages'] and  len(maindb['independent_indices'][indice]['mewma_messages'][duration_selected])>1 and pandas.isnull(maindb['independent_indices'][indice]['mewma_messages'][duration_selected][i])==False:
                        if ship_configs['equipment_anamoly']['MEWMA_CUMSUM_alpha1']['message'] in maindb['processed_daily_data'][indice]['mewma_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['equipment_anamoly']['MEWMA_CUMSUM_alpha1']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)
                        elif ship_configs['equipment_anamoly']['MEWMA_CUMSUM_alpha2']['message'] in maindb['processed_daily_data'][indice]['mewma_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['equipment_anamoly']['MEWMA_CUMSUM_alpha2']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)
                        elif ship_configs['equipment_anamoly']['MEWMA_CUMSUM_alpha3']['message'] in maindb['processed_daily_data'][indice]['mewma_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['equipment_anamoly']['MEWMA_CUMSUM_alpha3']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)


        elif function_name=="t2_limit_compare":
            ship_imo=function_args.get("ship_imo")
            date=function_args.get("date")
            parameter=function_args.get("parameter")
            duration=function_args.get("duration")
            function_response,ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected = fuction_to_call(ship_imo,date,parameter,duration)
            print("function responseeeee",function_response)
            messages.append(response_message) 
            messages.append({"role": "function","name": function_name,"content": function_response}) 
            second_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=messages,temperature=0,max_tokens = 200)
            second_query_new= "This is previous reply to the query that you gave"+" "+second_response["choices"][0]["message"]['content']+second_query
            second_messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating parameters beyond predefined thresholds that are told to you"},{"role": "user", "content": second_query_new}]
            Third_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=second_messages,temperature=0,max_tokens = 200)
            print("second",second_response)
            if duration_selected!=None:
                for i in range(0,3):    
                    if len(maindb['processed_daily_data'][parameter]['_t2_messages'][duration_selected])>1 and pandas.isnull(maindb['processed_daily_data'][parameter]['_t2_messages'][duration_selected][i])==False:
                        if ship_configs['parameter_anamoly']['T2_alpha1']['message'] in maindb['processed_daily_data'][parameter]['_t2_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['parameter_anamoly']['T2_alpha1']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)
                        elif ship_configs['parameter_anamoly']['T2_alpha2']['message'] in maindb['processed_daily_data'][parameter]['_t2_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['parameter_anamoly']['T2_alpha2']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)
                        elif ship_configs['parameter_anamoly']['T2_alpha3']['message'] in maindb['processed_daily_data'][parameter]['_t2_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['parameter_anamoly']['T2_alpha3']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)
        
        elif function_name=="indice_t2_limit_compare":
            ship_imo=function_args.get("ship_imo")
            date=function_args.get("date")
            indice=function_args.get("indice")
            duration=function_args.get("duration")
            function_response,ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected = fuction_to_call(ship_imo,date,indice,duration)
            print("function responseeeee",function_response)
            messages.append(response_message) 
            messages.append({"role": "function","name": function_name,"content": function_response}) 
            second_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=messages,temperature=0,max_tokens = 200)
            second_query_new= "This is previous reply to the query that you gave"+" "+second_response["choices"][0]["message"]['content']+second_query
            second_messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating indices beyond predefined thresholds that are told to you"},{"role": "user", "content": second_query_new}]
            Third_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=second_messages,temperature=0,max_tokens = 200)
            print("second",second_response)
            if duration_selected!=None:
                for i in range(0,3):    
                    if len(maindb['independent_indices'][indice]['_t2_messages'][duration_selected])>1 and pandas.isnull(maindb['independent_indices'][indice]['_t2_messages'][duration_selected][i])==False:
                        if ship_configs['equipment_anamoly']['T2_alpha1']['message'] in maindb['independent_indices'][indice]['_t2_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['equipment_anamoly']['T2_alpha1']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)
                        elif ship_configs['equipment_anamoly']['T2_alpha2']['message'] in maindb['independent_indices'][indice]['_t2_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['equipment_anamoly']['T2_alpha2']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)
                        elif ship_configs['equipment_anamoly']['T2_alpha3']['message'] in maindb['independent_indices'][indice]['_t2_messages'][duration_selected][i]:
                            gpt_db_message=ship_configs['equipment_anamoly']['T2_alpha3']['message']+"---"+str(Third_response["choices"][0]["message"]['content'])
                            print(gpt_db_message)

        elif function_name=="outlier_limit_compare":
            ship_imo=function_args.get("ship_imo")
            date=function_args.get("date")
            parameter=function_args.get("parameter")
            duration=function_args.get("duration")
            function_response,ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected = fuction_to_call(ship_imo,date,parameter,duration)
            print("function responseeeee",function_response)
            messages.append(response_message) 
            messages.append({"role": "function","name": function_name,"content": function_response}) 
            second_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=messages,temperature=0,max_tokens = 200)
            second_query_new= "This is previous reply to the query that you gave"+" "+second_response["choices"][0]["message"]['content']+second_query
            second_messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating parameters beyond predefined thresholds that are told to you"},{"role": "user", "content": second_query_new}]
            Third_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=second_messages,temperature=0,max_tokens = 200)
            print("second",second_response)
            if duration_selected!=None:
                if pandas.isnull(maindb['processed_daily_data'][parameter]['outlier_limit_msg'][duration_selected])==False:
                    if ship_configs['outlier_anamoly']['OutlierLimit']['message'] in maindb['processed_daily_data'][parameter]['outlier_limit_msg'][duration_selected]:
                        gpt_db_message=ship_configs['outlier_anamoly']['OutlierLimit']['message']+"---"+str(second_response["choices"][0]["message"]['content'])
                        print(gpt_db_message)

        elif function_name=="operational_limit_compare":
            ship_imo=function_args.get("ship_imo")
            date=function_args.get("date")
            parameter=function_args.get("parameter")
            duration=function_args.get("duration")
            function_response,ship_configs_collection,ship_configs,maindb_collection,maindb,duration_selected = fuction_to_call(ship_imo,date,parameter,duration)
            print("function responseeeee",function_response)
            messages.append(response_message) 
            messages.append({"role": "function","name": function_name,"content": function_response}) 
            second_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=messages,temperature=0,max_tokens = 200)
            second_query_new= "This is previous reply to the query that you gave"+" "+second_response["choices"][0]["message"]['content']+second_query
            second_messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating parameters beyond predefined thresholds that are told to you"},{"role": "user", "content": second_query_new}]
            Third_response = openai.ChatCompletion.create(model="gpt-3.5-turbo-0613",messages=second_messages,temperature=0,max_tokens = 200)
            print("second",second_response)
            if duration_selected!=None:
                if pandas.isnull(maindb['processed_daily_data'][parameter]['operational_limit_msg'][duration_selected])==False:
                    if ship_configs['outlier_anamoly']['OperationalLimit']['message'] in maindb['processed_daily_data'][parameter]['operational_limit_msg'][duration_selected]:
                        gpt_db_message=ship_configs['outlier_anamoly']['OperationalLimit']['message']+"---"+str(second_response["choices"][0]["message"]['content'])
                        print(gpt_db_message)

        
        return Third_response
    

# SPE CALL HERE
# print(run_conversation("for ship imo 9205926.    date= 2016-10-5 . parameter= pwr(Which is Main Engine Power) and duration= 6. Obtain the SPE value and  critical thresholds and alpha values from stored db.  I need you to compare the SPE value  with   critical threshold values corresponding to each of the given alpha values and tell me  what are  the nearest lower critical threshold  (“LCT”) and nearest upper critical threshold (“UCT”) (Be informed that LCT is a value that   is highest amongst the set of  all threshold values that are   values lower than the SPE value which makes the LCT value thus  chosen always lower than the SPE Value.   UCT is a value that is lowest amongst the set of  all threshold values that are  higher than the SPE value, which makes the UCT value thus chosen, always higher than the SPE value.)(Further, only for your information : Thresholds   for health of equipment are such that, alpha= 0.2 and alpha =0.05 indicate relative best and worst health thresholds respectively and alpha=0.1 in the middle. You need only to tell me SPE value, LCT and it’s corresponding alpha (if it exists) and UCT and it’s alpha(If it exits) and nothing else).give your final observation with minimum words.If Ewma is found to exceed the LCT, word your appropriate message expressing your carefully considered and worded and specific to this parameter, concern for deviation  of the Engine parameter (in this case Engine Power)  from it's expected value","give your final observations only, capturing  the essence,  with minimum word, and base it on previous response."))
"""responses
0.2   spe  0.1  0.05 
{"0.2": 0.6493, "0.1": 1.5596, "0.05": 2.7193}, "spe value": 0.9296}
First Response=The SPE value for the given ship (IMO 9205926) on October 5, 2016, with a duration of 6, is 0.9296. \n\nThe critical threshold values corresponding to the alpha values are as follows:\n- Alpha = 0.2: Critical threshold = 0.6493\n- Alpha = 0.1: Critical threshold = 1.5596\n- Alpha = 0.05: Critical threshold = 2.7193\n\nThe nearest lower critical threshold (LCT) is 0.6493, corresponding to alpha = 0.2. The nearest upper critical threshold (UCT) is 1.5596, corresponding to alpha = 0.1.\n\nIf the EWMA value exceeds the LCT (0.6493), it indicates a deviation of the Engine Power parameter from its expected value.
Second Response="The SPE value for the ship on October 5, 2016, with a duration of 6, is 0.9296. The critical threshold values for alpha = 0.2 and alpha = 0.1 are 0.6493 and 1.5596, respectively. The SPE value exceeds the lower critical threshold, indicating a deviation in the Engine Power parameter."


0.2  0.1  spe  0.05 
{"0.2": 0.6493, "0.1": 1.5596, "0.05": 2.7193}, "spe value": 2}
First Response=The SPE value for the Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, with a duration of 6, is 2. \n\nThe critical threshold values corresponding to the alpha values are as follows:\n- Alpha 0.2: 0.6493\n- Alpha 0.1: 1.5596\n- Alpha 0.05: 2.7193\n\nThe nearest lower critical threshold (LCT) is 1.5596, corresponding to alpha 0.1. The nearest upper critical threshold (UCT) is 2.7193, corresponding to alpha 0.05.\n\nIf the Ewma value exceeds the LCT, it indicates a concern for deviation of the Engine Power parameter from its expected value.
Second Response="The Main Engine Power parameter on ship IMO 9205926 exceeded the critical threshold value of 1.5596 on October 5, 2016, with a duration of 6. This indicates a significant deviation from the expected value and raises concerns about the performance of the engine."

0.2  0.1  0.05  spe
{"0.2": 0.6493, "0.1": 1.5596, "0.05": 2.7193}, "spe value": 3}
First Response=The SPE value for the Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, with a duration of 6, is 3. \n\nThe nearest lower critical threshold (LCT) is 2.7193, corresponding to an alpha value of 0.05. \n\nThere is no upper critical threshold (UCT) as all threshold values are lower than the SPE value. \n\nBased on the Ewma exceeding the LCT, there is a concern for deviation of the Engine Power parameter from its expected value.
Second Response="Concern: Deviation of Engine Power parameter from expected value."

"""

#EWMA CALL HERE
# print(run_conversation("for ship imo 9205926.    date= 2016-10-5 . parameter= pwr(Which is Main Engine Power) and duration= 6.Obtain the Ewma (Exponentially weighted moving average of the deviation of the parameter from it’s expected value) value and  critical thresholds and alpha values from stored db.  I need you to compare the Ewma value  with   critical threshold values corresponding to each of the given alpha values and tell me  what are  the nearest lower critical threshold  (“LCT”) and nearest upper critical threshold (“UCT”) (Be informed that LCT is a value that   is highest amongst the set of  all threshold values that are   values lower than the Ewma value which makes the LCT value thus  chosen always lower than the Ewma Value.   UCT is a value that is lowest amongst the set of  all threshold values that are  higher than the Ewma value, which makes the UCT value thus chosen, always higher than the Ewma value. )(Further, only for your information : Thresholds   for health of equipment are such that, alpha= 0.2 and alpha =0.05 indicate relative best and worst health thresholds respectively and alpha=0.1 is in between these two. You need only to tell me Ewma value, LCT and it’s corresponding alpha (if it exists) and UCT and it’s alpha(If it exits) and nothing else).give your final observation with minimum words.If Ewma is found to exceed the LCT,  word your appropriate message expressing your carefully considered and worded and specific to this parameter, concern for deviation  of the Engine parameter (in this case Engine Power)  from it's expected value","give your final observations only, capturing  the essence,  with minimum word, and base it on previous response."))
"""responses
 0.2   ewma  0.1  0.05 
{"limits corresponding to alpha": {"0.2": 0.0923, "0.1": 0.1022, "0.05": 0.1103}, "ewma value": 0.97}
First Response=The EWMA value for the Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, with a duration of 6, is 0.97. \n\nThe critical threshold values corresponding to different alpha values are as follows:\n- Alpha = 0.2: Critical threshold = 0.0923\n- Alpha = 0.1: Critical threshold = 0.1022\n- Alpha = 0.05: Critical threshold = 0.1103\n\nThe nearest lower critical threshold (LCT) is 0.0923 with an alpha value of 0.2. The nearest upper critical threshold (UCT) is 0.1022 with an alpha value of 0.1.\n\nBased on the comparison, the EWMA value of 0.97 exceeds the LCT of 0.0923. This indicates a concern for the deviation of the Main Engine Power parameter from its expected value.
Second Response=The Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, has exceeded the critical threshold of 0.0923 with an EWMA value of 0.97. This indicates a significant deviation from the expected value and raises concerns about the performance of the main engine.


0.2 0.1 ewma 0.05
{"limits corresponding to alpha": {"0.2": 0.0923, "0.1": 0.1022, "0.05": 0.1103}, "ewma value": 0.104}
First Response=The EWMA value for the parameter \"pwr\" (Main Engine Power) on ship IMO 9205926 on October 5, 2016, with a duration of 6, is 0.104. \n\nThe critical threshold values corresponding to different alpha values are as follows:\n- Alpha = 0.2: Critical threshold = 0.0923\n- Alpha = 0.1: Critical threshold = 0.1022\n- Alpha = 0.05: Critical threshold = 0.1103\n\nThe nearest lower critical threshold (LCT) is 0.1022, corresponding to alpha = 0.1. The nearest upper critical threshold (UCT) is 0.1103, corresponding to alpha = 0.05.\n\nBased on the comparison, the EWMA value does not exceed the LCT. Therefore, there is no concern for deviation of the Engine Power parameter from its expected value.
Second Response=The EWMA value for the Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, does not exceed the critical threshold. No deviation from the expected value is observed."


0.2  0.1  0.05 ewma
{"0.2": 0.0923, "0.1": 0.1022, "0.05": 0.1103}, "ewma value": 0.4}
First Response="The EWMA value for the parameter \"pwr\" (Main Engine Power) on ship IMO 9205926 on October 5, 2016, with a duration of 6, is 0.4. \n\nThe critical threshold values corresponding to different alpha values are as follows:\n- Alpha = 0.2: Critical threshold = 0.0923\n- Alpha = 0.1: Critical threshold = 0.1022\n- Alpha = 0.05: Critical threshold = 0.1103\n\nThe nearest lower critical threshold (LCT) is 0.1022, corresponding to alpha = 0.1. The nearest upper critical threshold (UCT) is 0.1103, corresponding to alpha = 0.05.\n\nObservation: The EWMA value exceeds the LCT, indicating a concern for deviation of the Engine Power parameter from its expected value."
Second Response="Observation: The EWMA value for the Main Engine Power parameter on October 5, 2016, exceeds the critical threshold of 0.1022, indicating a deviation from the expected value.

"""

# T2 CALL HERE
# print(run_conversation("for ship imo 9205926.    date= 2016-10-5 . parameter= pwr(Which is Main Engine Power) and duration= 6 .  Obtain the T2 value and  critical thresholds and alpha values from stored db.  I need you to compare the T2 value  with   critical threshold values corresponding to each of the given alpha values and tell me  what are  the nearest lower critical threshold  (“LCT”) and nearest upper critical threshold (“UCT”) (Be informed that LCT is a value that   is highest amongst the set of  all threshold values that are   values lower than the T2 value which makes the LCT value thus  chosen always lower than the T2 Value.   UCT is a value that is lowest amongst the set of  all threshold values that are  higher than the T2 value, which makes the UCT value thus chosen, always higher than the T2 value.)(Further, only for your information : Thresholds   for health of equipment are such that, alpha= 0.2 and alpha =0.05 indicate relative best and worst health thresholds respectively and alpha=0.1 in the middle. You need only to tell me T2 value, LCT and it’s corresponding alpha (if it exists) and UCT and it’s alpha(If it exits) and nothing else).give your final observation with minimum words.If T2 is found to exceed the LCT,word your appropriate message expressing your carefully considered and worded and specific to this parameter, concern for deviation  of the Engine parameter (in this case Engine Power)  from it's expected value","give your final observations only, capturing  the essence,  with minimum word, and base it on previous response."))
"""
T2  0.2  0.1  0.05
{"0.2": 6.08, "0.1": 7.93, "0.05": 9.69}, "t2 value": 2.4484}
First Response=The T2 value for the Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, with a duration of 6, is 2.4484. \n\nThe critical threshold values corresponding to the alpha values are as follows:\n- Alpha = 0.2: Critical threshold = 6.08\n- Alpha = 0.1: Critical threshold = 7.93\n- Alpha = 0.05: Critical threshold = 9.69\n\nThe nearest lower critical threshold (LCT) is 2.4484 (as it is the highest value lower than the T2 value), and its corresponding alpha is not provided. The nearest upper critical threshold (UCT) is 6.08 (as it is the lowest value higher than the T2 value), and its corresponding alpha is 0.2.\n\nObservation: The T2 value does not exceed the LCT, indicating that the Engine Power parameter is within the expected
Second Response=The T2 value for the Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, with a duration of 6, is 2.4484. This value does not exceed the nearest lower critical threshold (LCT), indicating that the Engine Power parameter is within the expected range.


0.2  T2  0.1  0.05
{"0.2": 6.08, "0.1": 7.93, "0.05": 9.69}, "t2 value": 7}
First Response=The T2 value for the Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, with a duration of 6, is 7. \n\nThe critical threshold values corresponding to the alpha values are as follows:\n- Alpha 0.2: 6.08\n- Alpha 0.1: 7.93\n- Alpha 0.05: 9.69\n\nThe nearest lower critical threshold (LCT) is 7.93, corresponding to alpha 0.1. The nearest upper critical threshold (UCT) is 9.69, corresponding to alpha 0.05.\n\nObservation: The T2 value of 7 exceeds the LCT of 7.93. This indicates a deviation of the Engine Power parameter from its expected value.
Second Response=Violation: The T2 value of 7 exceeds the LCT of 7.93, indicating a deviation in the Main Engine Power parameter on October 5, 2016.


0.2  0.1  T2  0.05
{"0.2": 6.08, "0.1": 7.93, "0.05": 9.69}, "t2 value": 8.5}
First Response=The T2 value for the Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, with a duration of 6, is 8.5. \n\nThe nearest lower critical threshold (LCT) is 7.93, corresponding to an alpha value of 0.1. \n\nThe nearest upper critical threshold (UCT) is 9.69, corresponding to an alpha value of 0.05. \n\nBased on the comparison, the T2 value exceeds the LCT, indicating a deviation of the Engine Power parameter from its expected value.
Second Response=The Main Engine Power parameter on ship IMO 9205926 exceeded the lower critical threshold on October 5, 2016, indicating a deviation from its expected value.


0.2  0.1  0.05  T2
{"0.2": 6.08, "0.1": 7.93, "0.05": 9.69}, "t2 value": 10}
First Response=The T2 value for the Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, with a duration of 6, is 10. \n\nThe critical threshold values corresponding to the alpha values are as follows:\n- Alpha 0.2: 6.08\n- Alpha 0.1: 7.93\n- Alpha 0.05: 9.69\n\nThe nearest lower critical threshold (LCT) is 9.69, corresponding to alpha 0.05. The nearest upper critical threshold (UCT) does not exist in this case.\n\nObservation: The T2 value exceeds the LCT, indicating a deviation of the Engine Power parameter from its expected value.
Second Response=Violation: The T2 value for the Main Engine Power parameter on ship IMO 9205926 on October 5, 2016, with a duration of 6, exceeds the critical threshold of 9.69 (alpha 0.05).

"""

# OUTLIER CALL HERE
# print(run_conversation("for ship imo 9205926.    date= 2016-10-5 . parameter= pwr(Which is Main Engine Power) and duration= 6 . Compare the outlier values and tell if it is oultier or not"))
"RESPONE==Based on the provided information, for the ship with IMO number 9205926 on October 5, 2016, the parameter \"pwr\" had a value of 5188. \n\nThe outlier range for this parameter, considering a duration of 6 days, is from null to 8668. Since the processed value of 5188 falls within this range, it is not considered an outlier."

# OPERATIONAL CALL HERE
# print(run_conversation("for ship imo 9205926.    date= 2016-10-5 . parameter= pwr(Which is Main Engine Power) and duration= 6. Compare the operational values and tell if it is oultier or not"))
"RESPONE==Based on the provided information, the operational parameter \"pwr\" for the ship with IMO number 9205926 on October 5, 2016, with a duration of 6, has a processed value of 5188. \n\nThe operational range for this parameter is not specified for the given date and duration. However, if the processed value falls within the range of 0 to 7880, it is considered within the operational limits. \n\nIn this case, the processed value of 5188 falls within the operational range, indicating that it is not an outlier and is within the predefined operational limits."
