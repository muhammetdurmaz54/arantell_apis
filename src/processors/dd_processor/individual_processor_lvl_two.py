# not react application backend, part of maindb process this code is for individual processor(formulas) when it is called second time


from msilib.schema import Error
import re
from numpy import zeros_like
import pandas   
    
class IndividualProcessorsTwo():


    def __init__(self,configs,md,ed):
        self.ship_configs = configs
        self.main_data= md
        self.equipment_data=ed
    
    def return_variable(self,string):
        "returns the variable(identifiers) in the formula"
        txt = string
        txt_search=[i for i in re.findall("[a-zA-Z0-9_]+",txt) if not i.isdigit()]
        return txt_search

    def base_formula_daily_data(self,formula_string,zero_error):
        "base formula function that is read from shipconfig. fetches variable data pressent in shipstatic and dailydata to populate maindb"
        list_var=self.return_variable(formula_string)
        static_data=self.ship_configs['static_data']
        main_data=self.main_data
        temp_dict={}
        
        for i in list_var:
            if i in static_data and pandas.isnull(static_data[i])==False:
                temp_dict[i]=static_data[i]['value']
            elif i in main_data and "param_dummy_val" in main_data[i] and pandas.isnull(main_data[i]['param_dummy_val'])==False:
                temp_dict[i]=main_data[i]['param_dummy_val']
            elif i in main_data and pandas.isnull(main_data[i]['processed'])==False:
                temp_dict[i]=main_data[i]['processed']
            elif i in self.equipment_data and pandas.isnull(self.equipment_data[i]['processed'])==False:
                temp_dict[i]=self.equipment_data[i]['processed']
            else:
                temp_dict[i]=None
        if zero_error==False:
            return eval(self.eval_val_replacer(temp_dict, formula_string))
        elif zero_error==True:
            values_string=self.eval_val_replacer(temp_dict, formula_string)
            er_temp=0
            for v in values_string.split("/"):
                try:
                    t=eval(v)
                    if er_temp==0:
                        er_temp=t
                    elif t>0:
                        er_temp=er_temp/t
                except:
                    er_temp=None
            return er_temp

        
    def eval_val_replacer(self,temp_dict, string):
        "replaces the identifier with their value"
        for key in temp_dict.keys():
            string = re.sub(r"\b" + key + r"\b", str(temp_dict[key]), string, flags=re.I)
        return string

    def base_individual_processor(self,identifier,base_dict):
        "base individual processor for those which has a common structure  or repetetive structure and formulas based on daily data availability. not for those which includes degree or beaufort function"
        base_dict=base_dict
        derived=self.ship_configs['data'][identifier]['Derived']
        source_identifier=self.ship_configs['data'][identifier]['source_idetifier']
        static_data=self.ship_configs['data'][identifier]['static_data']

        if derived==True and pandas.isnull(derived)==False:
            if pandas.isnull(source_identifier):
                if pandas.isnull(static_data)==False:
                    try:
                        base_dict['processed']=self.base_formula_daily_data(static_data,False)
                        if pandas.isnull(base_dict['processed'])==False:
                            base_dict['is_processed']=True
                        else:
                            base_dict['is_processed']=False
                    except ZeroDivisionError:
                        base_dict['processed']=self.base_formula_daily_data(static_data,True)
                        base_dict['is_processed']=True
                    except:
                        base_dict['processed']=None
                        base_dict['is_processed']=False
        # print(base_dict['processed'])
        return base_dict