# from dotenv import load_dotenv
# import sys
# import os
# from pymongo import MongoClient
# import pandas as pd
# from pymongo import ASCENDING
# import base64





# # login_info = database.get_collection("login_info")

# # login_info_contota = login_info.find({"organization_id": 77952})[0]
# # encoded_pass=login_info_contota['password']

# # print(str(base64.b64decode(encoded_pass)))

















# #self.maindb.update_many({},{"$set":{"processed_daily_data.cp_speed.identifier":"cp_speed"}})
# def updated(ship_imo,name):
#     ship_config = database.get_collection("ship")
#     # local_ship_config = ship_config.find({"ship_imo": ship_imo})[0]
#     daily_data = database.get_collection("daily_data")
#     # daily_data_docs = daily_data.find({"ship_imo": ship_imo})

#     # shared_ship = shared_database.get_collection("ship")
#     # shared_ship_config = shared_ship.find({"ship_imo": ship_imo})[0]

#     # shared_ship_config['t2_limits']=local_ship_config['t2_limits']
#     # shared_ship_config['t2_limits_indices']=local_ship_config['t2_limits_indices']
#     # shared_ship_config['ewma_limits']=local_ship_config['ewma_limits']
#     # shared_ship_config['spe_limits']=local_ship_config['spe_limits']
#     # shared_ship_config['mewma_limits']=local_ship_config['mewma_limits']
#     # shared_ship_config['spe_limits_indices']=local_ship_config['spe_limits_indices']
#     # shared_ship_config['common_col']=local_ship_config['common_col']


#     # shared_ship.delete_one({"ship_imo": ship_imo})
#     # print("deleteddd")
#     # shared_ship.insert_one(shared_ship_config)
#     # print("inserted")

    
#     ship_config.update_one({"ship_imo": ship_imo},{"$set":{"ship_name":name,'static_data.ship_name.value':name}})
#     daily_data.update_many({"ship_imo": ship_imo},{"$set":{"ship_name":name}})
#     # ship_config.delete_one({"ship_imo": ship_imo})
#     # print("deleteddd")
#     # ship_config.insert_one(local_ship_config)
#     # print("inserted")





#     # main_dict_list = maindb.find({"ship_imo": ship_imo})
#     # base_line_data=pd.read_excel("contota_baseline_foc.xlsx",engine='openpyxl').fillna("")
#     # print(base_line_data)
#     # voyage_dates=[]
#     # for i in range(0,local_main_dict_list.count()):
#     #     print(i)
#     #     shared_maindb.insert_one(local_main_dict_list[i]).inserted_id
#     #     print("inserted")
#         # print(main_dict_list[i]['processed_daily_data']['rep_dt']['processed'],base_line_data['date'].iloc[i])

#         # print(main_dict_list[i]['processed_daily_data']['rep_dt']['processed'],main_dict_list[i]['processed_daily_data']['voyage']['processed'])
#         # print(main_dict_list[i]['processed_daily_data']['voyage']['processed'][:2])
#         # voyage_dates.append(main_dict_list[i]['processed_daily_data']['rep_dt']['processed'])
#         # if i!=0:
#         #     # prev_date=main_dict_list[i-1]['processed_daily_data']['voyage']['processed']
#         #     # today_date=main_dict_list[i]['processed_daily_data']['voyage']['processed']
#         #     # if prev_date.endswith("L") and today_date.endswith("B"):
#         #     #     print(voyage_dates[0],voyage_dates[-2])
#         #         # maindb.update_one(maindb.find({"ship_imo": ship_imo}).sort('final_rep_dt', ASCENDING)[i-1],{"$set":{"processed_daily_data.voyage.fromdate":voyage_dates[0],"processed_daily_data.voyage.todate":voyage_dates[-2]}})
            


#         #     voyage_dates=[]

#         # maindb.update_one(maindb.find({"ship_imo": ship_imo})[i],{"$set":{"baseline_pred.main_lsfo":base_line_data['foc_baseline'].iloc[i]}})
#     print("final done")


# updated(6185798,"MARINE TRUFON")



