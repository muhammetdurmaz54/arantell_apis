# react application backend for initiating maindb process when user gives inputs in frontend spreadsheet

from pymongo import MongoClient
import os
from src.processors.dd_processor.daily_maindbtwo import MainDB
from datetime import datetime
from threading import Thread

class MainDbRunner():
    def __init__(self, date, ship_imo, timestamp):
        self.ship_imo = int(ship_imo)
        self.date = date
        self.timestamp = int(timestamp) if timestamp != None else None
    
    def check_fuel_and_engine_availability_and_run(self):
        client = MongoClient(os.getenv('MONGODB_ATLAS'))
        db=client.get_database("aranti")
        database = db
        daily_data_collection = database.get_collection("daily_data")

        try:
            try:
                try:
                    try:
                        rep_dt = datetime.strptime(self.date, '%d/%m/%y %H:%M:%S')
                    except:
                        rep_dt = datetime.strptime(self.date, '%d/%m/%Y %H:%M:%S')
                except:
                    try:
                        rep_dt = datetime.strptime(self.date, '%d/%m/%y')
                    except:
                        rep_dt = datetime.strptime(self.date, '%d/%m/%Y')
            except:
                try:
                    rep_dt = datetime.strptime(self.date, '%d-%m-%y %H:%M:%S')
                except:
                    rep_dt = datetime.strptime(self.date, '%d-%m-%Y %H:%M:%S')
        except:
            try:
                rep_dt = datetime.strptime(self.date, '%d-%m-%y')
            except:
                rep_dt = datetime.strptime(self.date, '%d-%m-%Y')

        rep_dt_list = daily_data_collection.distinct("data.rep_dt")
        for dt in rep_dt_list:
            if dt.date() == rep_dt.date():
                for doc in daily_data_collection.find({"ship_imo": self.ship_imo, "data.rep_dt": dt, "timestamp": self.timestamp, "historical": False}, {"nav_data_available": 1, "engine_data_available": 1}):
                #     if doc['nav_data_available'] == True and doc['engine_data_available'] == True:
                    print("BEFORE MAIN DB!!!!!!")
                    run(self.ship_imo, self.timestamp, dt)
                        # thread = Thread(target=run, args=(self.ship_imo, self.timestamp, rep_dt,))
                        # thread.start()
                        # thread.join()

                        # self.run(rep_dt)
                    return "All Done!!!"
                    # elif doc['nav_data_available'] == True and doc['engine_data_available'] == False:
                    #     return "Engine Data not available!"
                    # elif doc['nav_data_available'] == False and doc['engine_data_available'] == True:
                    #     return "Fuel Data not available!"
                    # else:
                    #     return "Can't run function!"
    
def run(ship_imo, timestamp, rep_dt):
    obj = MainDB(ship_imo, timestamp, rep_dt)
    obj.get_ship_configs()
    print("AFTER MAIN DB!!!!!!")
    first_maindict=obj.ad_all()
    calc_i_cp_main_dict=obj.add_calc_i_cp(first_maindict)
    equipment_values_main_dict=obj.equipment_values(calc_i_cp_main_dict)
    lvl_two_main_dict=obj.maindb_lvl_two(equipment_values_main_dict)
    base_dataframe_one=obj.create_base_dataframe(lvl_two_main_dict)
    outlier_main_dict=obj.update_outlier_maindb_alldoc(base_dataframe_one)
    good_voyage_main_dict=obj.update_good_voyage(outlier_main_dict)
    preds_main_dict=obj.update_maindb_predictions_alldoc(good_voyage_main_dict)
    main_fuel_spe=obj.update_main_fuel_spe(preds_main_dict)

    sfoc_spe=obj.update_sfoc_spe(main_fuel_spe)

    avg_hfo_spe=obj.update_avg_hfo_spe(sfoc_spe)


    indices_preds_main_dict=obj.update_indices(avg_hfo_spe)

    uni_limit=obj.universal_limit(indices_preds_main_dict)
    uni_indice_limit=obj.universal_indices_limits(uni_limit)
    ewma_limit=obj.ewma_limits(uni_indice_limit)
    indice_ewma=obj.indice_ewma_limit(ewma_limit)

    cp_msg_main_dict=obj.update_cp_msg(indice_ewma)
    final_main_dict=obj.anamolies_by_config(cp_msg_main_dict)
    create_maindb_update_shipconfig=obj.final_maindb_config(final_main_dict)



# start_time = time.time()

# # daily_obj=DailyInsert('F:\Afzal_cs\Internship\Arvind data files\RTM FUEL.xlsx','F:\Afzal_cs\Internship\Arvind data files\RTM ENGINE.xlsx',9591301,True)
# # daily_obj.do_steps()
# # maindb = database.get_collection("Main_db")

# # maindb.delete_many({"ship_imo": 9591301,"processed_daily_data.rep_dt.processed":datetime(2017,1,1,12)})
# obj=MainDB(9591301,None,datetime.strptime('14/12/16 12:00:00','%d/%m/%y %H:%M:%S'))
# obj.get_ship_configs()
# # obj.get_main_db(0)
# first_maindict=obj.ad_all()
# # #initialize maindb with handwritten formulas draftmean=(dft_aft+dt_fwd)/2 (dailydata)
# calc_i_cp_main_dict=obj.add_calc_i_cp(first_maindict)
# # #adding calc i cp variable values in respective identifier example:speed_sog_calc=speed_sog{speed_sog_calc:value}
# equipment_values_main_dict=obj.equipment_values(calc_i_cp_main_dict)
# # #updating equipment values 0 or 1 here
# lvl_two_main_dict=obj.maindb_lvl_two(equipment_values_main_dict)
# # #same as ad_all (gets the value from maindb)
# # #initial population done (remove date condition on find  before uploading in aws)
# base_dataframe_one=obj.create_base_dataframe(lvl_two_main_dict)
# # # creates dataframe of all identifiers (currently all good vayage true)
# outlier_main_dict=obj.update_outlier_maindb_alldoc(lvl_two_main_dict)
# # #outlier (both outlier 1 and 2 inside this) and (remove date condition on find  before uploading in aws)
# good_voyage_main_dict=obj.update_good_voyage(outlier_main_dict)
# # #good voyage tag created here essential for predictions process
# # base_dataframe_two=obj.create_base_dataframe(good_voyage_main_dict)    #not needed in dailydata processing bcz it is added for historical process(two phase creating dataframe once when good data voyage was not called and one after called)
# # #call again bcz after running good voyage function now good voyage values will be false in some rows so good data for prediction will be selected now
# preds_main_dict=obj.update_maindb_predictions_alldoc(good_voyage_main_dict)
# # #predictions, spe, t2, ewma, cumsum all done here (remove date condition on find  before uploading in aws)
# main_fuel_spe=obj.update_main_fuel_spe(preds_main_dict)

# sfoc_spe=obj.update_sfoc_spe(main_fuel_spe)

# avg_hfo_spe=obj.update_avg_hfo_spe(sfoc_spe)


# indices_preds_main_dict=obj.update_indices(avg_hfo_spe)
# # #creating indices as well as prediction, spe, t2, mewma, mcumsum, all done here

# uni_limit=obj.universal_limit(indices_preds_main_dict)
# uni_indice_limit=obj.universal_indices_limits(uni_limit)
# ewma_limit=obj.ewma_limits(uni_indice_limit)
# indice_ewma=obj.indice_ewma_limit(ewma_limit)

# # main_fuel_main_dict=obj.update_main_fuel(indices_preds_main_dict)
# # #backcalculationg main fuel by given furlmula (all values which are created in predictions processe will be backcalculated with same formula)
# # sfoc_main_dict=obj.update_sfoc(main_fuel_main_dict)
# # # #backcalculating 
# # avg_hfo_main_dict=obj.update_avg_hfo(sfoc_main_dict)
# # # #Backcalculating
# cp_msg_main_dict=obj.update_cp_msg(indice_ewma)
# final_main_dict=obj.anamolies_by_config(cp_msg_main_dict)
# create_maindb_update_shipconfig=obj.final_maindb_config(final_main_dict)

# #temporarily added for checking spe and ewma using a diferent dataframe and formula
# # done till here  

# end_time=time.time()
# print(end_time-start_time)
