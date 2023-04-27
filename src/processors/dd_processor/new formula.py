import math
import pandas


def to_wind_velocity(x):
    #Takes w_force(observed) as input, converts into knots scale
        beaufort = {0: 0,
                1: 1.5,
                2: 5,
                3: 8.5,
                4: 13.5,
                5: 19,
                6: 24.5,
                7: 30.5}  

        for k, v in beaufort.items():
            if int(x) == k:
                return v

def to_degree(x):
    #converts degrees in 16 scale format to angles
        degrees = {'N': '0',
                'E': '90',
                'S': '180',
                'W': '270',
                'NE': '45',
                'SE': '135',
                'SW': '225',
                'NW': '315',
                'NNE': '22',
                'ENE': '67',
                'ESE': '112',
                'SSE': '157',
                'SSW': '202',
                'WSW': '247',
                'WNW': '292',
                'NNW': '337'}

        for k, v in degrees.items():
            if x == k:
                return int(v)

w_velocity_list=[]
w_rel_velocity_list=[]
w_dir_rel_list=[]
def w_dir_rel_processor(data,ind):
        try:
            # print("w_dir_deg",data['w_dir_deg'][ind])
            # print('vessel_head',data['vessel_head'][ind])
            wind_dir_deg=data['w_dir_deg'][ind]
            numerator=to_wind_velocity(data['w_force'][ind])*math.sin(math.radians(wind_dir_deg-data['vessel_head'][ind]))
            
            velocity=to_wind_velocity(data['w_force'][ind])
            denominator=data['speed_sog_calc'][ind]+((to_wind_velocity(data['w_force'][ind])*math.cos(math.radians(wind_dir_deg-data['vessel_head'][ind]))))
        
            m=0
            if denominator<0:
                m=1
            processed=math.atan(numerator/denominator)*57.3+(180*m)
            # print("atan",math.atan(numerator/denominator)*57.3)
        except:
            processed=None
            velocity=None
        # print("w_dir_rel",processed)
        return processed,velocity          
        
def w_rel_velocity_processor(data,ind):
    try:
        wind_dir_deg=data['w_dir_deg'][ind]
        processed=math.sqrt(to_wind_velocity(data['w_force'][ind])**2+data['speed_sog_calc'][ind]**2+to_wind_velocity(data['w_force'][ind])*data['speed_sog_calc'][ind]*math.cos(math.radians(wind_dir_deg-data['vessel_head'][ind])))
    except:
        processed=None
    return processed 

def w_rel_0_processor(data,ind):   
    # print(w_dir_rel_processor(data,ind))  
    try:    
        dir_deg,velocity=w_dir_rel_processor(data,ind)
        w_rel_velocity=w_rel_velocity_processor(data,ind)
        processed=math.cos(math.radians(dir_deg)) *w_rel_velocity
        w_velocity_list.append(velocity)
    except:
        processed=None
        w_velocity_list.append(None)
        w_rel_velocity=None
        dir_deg=None
    w_rel_velocity_list.append(w_rel_velocity)
    w_dir_rel_list.append(dir_deg)
    return processed
    
def w_rel_90_processor(data,ind):
    try:
        dir_deg,velocity=w_dir_rel_processor(data,ind)
        w_rel_velocity=w_rel_velocity_processor(data,ind)
        processed=math.sin(math.radians(dir_deg))*w_rel_velocity
    except:
        processed=None
    return processed


stw_list=[]
current_rel_0=[]
current_rel_90=[]


def speed_stw_calc_processor(data,ind):
        # speed_stw=None
        #curfavag is given direction(N E W S) convert to degree and subtract vessel head(spe3ed sog - cos(subtractedvalue)) and curfavag none by default currentdir rel =0
        try:
            speed_sog=None    
            if pandas.isnull(data['stm_hrs'][ind])==False and pandas.isnull(data['dst_last'][ind])==False:
                if data['stm_hrs'][ind]==0:
                    speed_sog=data['dst_last'][ind]/0.1
                else:
                    speed_sog=data['dst_last'][ind]/data['stm_hrs'][ind]
            report=data['curfavag'][ind]
            curknots=data['curknots'][ind]
            # print(report)
            if pandas.isnull(report)==True:
                current_dir_rel=0
                processed=speed_sog-round(curknots*(math.cos(math.radians(current_dir_rel))))
                current_rel_0.append(round(curknots*(math.cos(math.radians(current_dir_rel)))))
                current_rel_90.append(round(curknots*(math.sin(math.radians(current_dir_rel)))))
            else:
                if report=='+':
                    current_dir_rel=180
                elif report=='-':
                    current_dir_rel=0
                elif report==0 or report==180:
                    current_dir_rel=report 
                else:    
                    vessel_head=data['vessel_head'][ind]
                    current_dir_rel=to_degree(report)-vessel_head
                processed=data['speed_sog_calc'][ind]-round(curknots*(math.cos(math.radians(current_dir_rel))))
                current_rel_0.append(round(curknots*(math.cos(math.radians(current_dir_rel)))))
                current_rel_90.append(round(curknots*(math.sin(math.radians(current_dir_rel)))))
            # print(report,processed)
        except:
            processed=None
            current_rel_90.append(None)
            current_rel_0.append(None)
        stw_list.append(processed)
        
        return processed

data=pandas.read_csv("F:/Afzal_cs/Internship/arantell_apis-main/src/processors/exceldatafiles/severn_full_data.csv")
w_rel_0_list=[]
w_rel_90_list=[]
for ind in data.index:
    # print(data[['w_dir_deg','w_force','vessel_head','speed_sog_calc']][ind])
    w_rel_0_list.append(w_rel_0_processor(data,ind))
    w_rel_90_list.append(w_rel_90_processor(data,ind))
    speed_stw_calc_processor(data,ind)

data['w_rel_0_newFormula']=w_rel_0_list
data['w_rel_90_newFormula']=w_rel_90_list
data['wind_velocity']=w_velocity_list
data['wind_rel_velocity']=w_rel_velocity_list
data['speed_stw_new']=stw_list
data['w_dir_rel_new']=w_dir_rel_list
data['current_rel_90_new']=current_rel_90
data['current_rel_0_new']=current_rel_0
# print(w_rel_0_list)
# print(w_rel_90_list)
# print(w_velocity_list)
# print(w_rel_velocity_list)
# print(stw_list)
data.to_csv("F:/Afzal_cs/Internship/arantell_apis-main/src/processors/exceldatafiles/severn_full_data_newformulas_current.csv")
