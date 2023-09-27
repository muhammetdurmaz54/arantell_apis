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


# open_ai_test_key='sample'
# llm = OpenAI(openai_api_key=open_ai_test_key)
# chat_model = ChatOpenAI(openai_api_key=open_ai_test_key)
# openai.api_key=open_ai_test_key
# query=sys.argv[1]

# print('open ai',llm.predict(query))
# print('chat open ai',chat_model.predict(query))

# response=openai.ChatCompletion.create(
#     model='gpt-3.5-turbo',
#     messages=[
#         {'role':'user','content':i have spe of 4.5 which of the limit thresholds it falls under and these corresponds to what alpha, what is the implication of this}
#     ]
# )

# print(response)

file = open('maindblocal_welland.json')
db_data = json.load(file)
print(len(db_data))
 
open_ai_test_key='sample'
openai.api_key=open_ai_test_key

def spe_limit_compare(val,spe_data,alpha):
    spe_data=list(spe_data)
    alpha=list(alpha)
    print(val,spe_data,alpha)
    print(type(val),type(spe_data),type(alpha))
    spe_std_val=np.std(spe_data)
    mean_val=np.mean(spe_data)
    print("mean_val",mean_val)
    var_sped=np.var(spe_data)
    print("std-dev",np.std(spe_data))
    print("var_sped",var_sped)
    spe_h_val=2*(mean_val**2)/(var_sped)
    spe_g_val=(var_sped)/(2*(mean_val))
    spe_y_limit_array={}
    spe_info={}
    for alphas in alpha:
        print(alphas)
        chi_val=st.chi2.ppf(1-alphas, spe_h_val)
        spe_limit_g_val=spe_g_val*chi_val
        print(spe_limit_g_val)
        if alphas==0.05:
            spe_info['0.05']=spe_limit_g_val
        if alphas==0.1:
            spe_info['0.1']=spe_limit_g_val
        if alphas==0.2:
            spe_info['0.2']=spe_limit_g_val
    print(spe_info)
    return json.dumps(spe_info)

def get_location(ship):
    return json.dumps({"location":"Canada"})

def get_cost(materials):
    return json.dumps({"ship_materials":800})

  
def run_conversation(query):
    messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating parameters beyond predefined thresholds that are told to you"},{"role": "system", "content": "pwr values are 3200,3800,4000,4200"},{"role": "user", "content": query}]
    functions = [
        {
            "name": "spe_limit_compare",
            "description": "compare spe with respect to its limit/thresholds. Dont call this function to generate or calculate anything, only compare",
            "parameters": {
                "type": "object",
                "properties": {
                    "speval": {
                        "type": "string",
                        "description": "The value of spe, e.g. 1,2,0.5",
                    },
                    "spedata": {
                        "type": "array",
                        "items": {"type": "number" },
                        "description": "list of spe values, e.g. [1,2,0.5]",
                    },
                    "alpha": {
                        "type": "array",
                        "items": {"type": "number" },
                        "description": "list of alpha values, e.g. [1,2,0.5]",
                    }
                },
                "required": ["speval","spedata","alpha"],
            },
        },
        {
            "name": "get_location",
            "description": "get real time location of ship",
            "parameters": {
                "type": "object",
                "properties": {
                    "ship": {
                        "type": "string",
                        "description": "The name of ship or imo of ship, e.g. welland, ATM BOOK, 9205926",
                    }
                },
                "required": ["ship"],
            },
        },
        {
            "name": "get_cost",
            "description": "get estimated cost of materials that user would like to purchase",
            "parameters": {
                "type": "object",
                "properties": {
                    "materials": {
                        "type": "string",
                        "description": "materials to be puchased, e.g. mobile, laptops etc",
                    }
                },
                "required": ["materials"],
            },
        }


    ]
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo-0613",
        messages=messages,
        functions=functions,
        function_call="auto", 
        temperature=0,
    )
    print('response',response)
    response_message = response["choices"][0]["message"]
    if response_message.get("function_call"):
        available_functions = {
            "spe_limit_compare": spe_limit_compare,
            "get_location":get_location,
            "get_cost":get_cost
        } 
        function_name = response_message["function_call"]["name"]
        fuction_to_call = available_functions[function_name]
        function_args = json.loads(response_message["function_call"]["arguments"])
        if function_name=="spe_limit_compare":
            function_response = fuction_to_call(
                val=function_args.get("speval"),
                spe_data=function_args.get("spedata"),
                alpha=function_args.get("alpha"),
            )
        elif function_name=="get_location":
            function_response = fuction_to_call(
                ship=function_args.get("ship")
            )
        elif function_name=="get_cost":
            function_response = fuction_to_call(
                materials=function_args.get("materials")
            )
        print("function responseeeee",function_response)
        messages.append(response_message) 
        messages.append(
            {
                "role": "function",
                "name": function_name,
                "content": function_response,
            }
        ) 
        second_response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo-0613",
            messages=messages,
            temperature=0,
            max_tokens = 2000,
        ) 
        return second_response
    


print(run_conversation("my spe value is 7.2.    spe data =[4,3,5,4,6,5,7,8] . Take alpha =[0.05,0.1,0.2]. For spe data,  Calculate the critical threshold values corresponding to each of the given alpha values.   Be reminded that thresholds   for health of equipment are such that, alpha= 0.2 and alpha =0.05 indicate relative best and worst health boundaries respectively. For the given alpha values identify the nearest lower critical threshold and nearest upper critical threshold (if they exist). Tell me  what is your interpretation with regards to this outcome? Remember that you need to  mention in your response only the alpha values for only the nearest upper threshold (if it exists} to my spe value and only the nearest lower threshold (If it exists) tor my spe value. Do not mention the alpha values  for  threshold values greater than nearest upper threshold and lower than nearest lower threshold, should they exist. Further I tell you that the SPE pertains to exhaust gas temperature of Ship's Marine Diesel engine where high SPE means higher exhaust temperature than predicted. Given this background please give your interpretation of the SPE outcome given to you in terms of the health of the diesel engine."))
# print(run_conversation("can you give current location of ship welland"))




















# MONGODB_URI = "mongodb://localhost:27017/aranti"
# client = MongoClient(MONGODB_URI)
# db=client.get_database("aranti")
# database = db


# def run_gpt_conv(query):
#     messages = [{"role": "system", "content": "You are a expert  marine engineer and expert statistician who interprets the implications of violations and exceedances of daily operating parameters beyond predefined thresholds that are told to you"},{"role": "user", "content": query}]
    
#     response = openai.ChatCompletion.create(
#         model="gpt-3.5-turbo-0613",
#         messages=messages, 
#     )
#     print('response dididid',response['choices'][0]['message']['content'])
#     return response['choices'][0]['message']['content']


# shiplocal = database.get_collection("ship")
# shiplocaldocs = shiplocal.find({"ship_imo": 9205926})[0]
# maindblocal = database.get_collection("Main_db")
# maindblocaldocs = maindblocal.find({"ship_imo": 9205926,"processed_daily_data.rep_dt.processed":{"$lte":datetime(2016,10,6,12),"$gte":datetime(2016,10,4,12)}})


# month_dict=['m6','m12','ly_m6','ly_m12']
# alpha_dict={'zero_two':'0.2','zero_one':'0.1','zero_zero_five':'0.05'}
# gpt_msg={}
# for mon in month_dict:
#     spe_alpha_dict={}
#     spe_limit_mon=shiplocaldocs['spe_limits']['pwr'][mon]
#     # print(mon,spe_limit_mon)
#     spe_val=maindblocaldocs[0]['processed_daily_data']['pwr']['SPEy'][mon]
#     for alp in spe_limit_mon:
#         # print(mon,spe_limit_mon[alp],spe_val)
#         spe_alpha_dict[alpha_dict[alp]]=spe_limit_mon[alp]
#     gpt_msg[mon]=spe_alpha_dict
#     # print(mon,spe_alpha_dict,spe_val)
#     dos="my spe value is {}  thresholds with respect to alpha={}".format(spe_val,spe_alpha_dict)
#     query=" .for the critical threshold values corresponding to each of the given alpha values.   Be reminded that thresholds   for health of equipment are such that, alpha= 0.2 and alpha =0.05 indicate relative best and worst health boundaries respectively. For the given alpha values identify the nearest lower critical threshold and nearest upper critical threshold (if they exist). Tell me  what is your interpretation with regards to this outcome? Remember that you need to  mention in your response only the alpha values for only the nearest upper threshold (if it exists} to my spe value and only the nearest lower threshold (If it exists) tor my spe value. Do not mention the alpha values  for  threshold values greater than nearest upper threshold and lower than nearest lower threshold, should they exist. Further I tell you that the SPE pertains to exhaust gas temperature of Ship's Marine Diesel engine where high SPE means higher exhaust temperature than predicted. Given this background please give your interpretation of the SPE outcome given to you in terms of the health of the diesel engine."
#     # print("queryyyyyyyyyy",dos+query)
#     gpt_response=run_gpt_conv(dos+query)
#     gpt_msg[mon]=gpt_response
# print(gpt_msg)
# maindblocal.update_one(maindblocal.find({"ship_imo": 9205926,"processed_daily_data.rep_dt.processed":{"$lte":datetime(2016,10,6,12),"$gte":datetime(2016,10,4,12)}})[0],{"$set":{"processed_daily_data.pwr.gpt_msg":gpt_msg}})
