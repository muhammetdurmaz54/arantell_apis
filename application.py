import sys
import os
# sys.path.insert(1,"D:\\Internship\\Repository\\Aranti\\arantell_apis")
from flask import Flask, app, request, flash, redirect, url_for
from werkzeug.utils import secure_filename
from flask_pymongo import PyMongo
from src.processors.config_extractor.trends_process import TrendsExtractor
from src.processors.config_extractor.dailyReport_process import DailyReportExtractor
from src.processors.config_extractor.interactive_process import InteractiveExtractor
from src.processors.config_extractor.interactive_stats_process import InteractiveStatsExtractor
from src.processors.config_extractor.overview_process import OverviewExtractor
from src.processors.config_extractor.configurator import Configurator
from src.processors.config_extractor.extract_config_new import ConfigExtractor
from src.processors.dd_extractor.extractor_new import DailyInsert
from src.processors.dd_processor.maindb import MainDB
from bson.json_util import loads
from flask_cors import cross_origin, CORS
import zipfile

UPLOAD_FOLDER = 'D:/Internship/Repository/Arantell/new-front-end/front_end/new-front-end/src/shared/'
ALLOWED_EXTENSIONS = {'zip'}

default_config = {'MONGODB_SETTINGS': {
    'db': 'test_db',
    'host': 'localhost',
    'port': 27017,
    'alias': 'default'
},
# 'MONGO_URI': 'mongodb://localhost:27017/aranti',
'MONGO_URI': 'mongodb+srv://iamuser:iamuser@democluster.lw5i0.mongodb.net/test?ssl=true&ssl_cert_reqs=CERT_NONE',
# 'MONGO_URI': 'mongodb://iamuser:iamuser@democluster.lw5i0.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
'CORS_HEADERS': {
    'ACCESS_CONTROL_ALLOW_ORIGIN': '*',
    # 'ACCESS_CONTROL_ALLOW_CREDENTIALS': True
},
'UPLOAD_FOLDER': UPLOAD_FOLDER,
'SECRET_KEY': 'super_secret_key'
}

application = Flask(__name__)
application.config.update(default_config)
CORS(application)
mongo = PyMongo(application)

@application.route('/', methods=['GET'])
@cross_origin()
def getStatus():
    return '''
        <!doctype html>
        <h1>All Fine!!!</h1>
    '''

@application.route('/ship_info', methods=['GET'])
@cross_origin()
def get_ship_info():
    configuration = Configurator(9591301)
    ship_configs = configuration.get_ship_configs()
    result = configuration.get_dict_for_ships()
    # result = configuration.get_generic_group_selection()
    return {'Result': result}

@application.route('/trends', methods=['GET', 'POST'])
@cross_origin()
def getTrends():
    ''' Handle requests from the Trends Page'''
    args=request.args
    # formargs = request.form
    print(args)
    ship_imo = args['ship_imo']
    group = args['group']
    duration = args['duration']
    include_outliers = args['include_outliers'] if 'include_outliers' in args else 'false'
    compare = args['compare'] if 'compare' in args else 'None'
    anomalies = args['anomalies'] if 'anomalies' in args else "true"
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
    print("INDIVIDUAL PARAMETERS !!!!!!!", individual_parameters)
    # individual_parameters_list = individual_parameters.replace(']', '').replace('[', '').split(',')
    # print(type(individual_parameters))
    # print("INDIVIDUAL B4!!!!!!!!", individual_parameters)
    # if group == '':
    #     res = TrendsExtractor(ship_imo, group, duration, individual_parameters, include_outliers)
    #     individual_groups = res.do_steps()
    #     return {'Individual Groups': individual_groups}
    # else:
    if include_outliers == 'true':
        res = TrendsExtractor(ship_imo, group, duration, individual_parameters, include_outliers, compare, anomalies)
        print("START DO STEPS")
        if 'Lastyear' in duration:
            groupList, mainresult, expresult, lowerresult, upperresult, lyexpresult, lylowerresult, lyupperresult, outlierresult, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict = res.do_steps()
            print("END DO STEPS")
            height = res.get_chart_height(groupList)
            fuelConsRes = res.process_fuel_consumption()
            # individual_params = res.individual_parameters()
            count=0
            for i in groupList:
                if i['group_availability_code']%10 == 0:
                    count = count + 1
            if('Multi Axis' in group):
                namesList = res.get_short_names_list_for_multi_axis()
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult, 'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, 'Short Names': namesList, "Chart Height": height, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
                # grpList = res.get_generic_group_list()
            variables_list = res.variable_list_according_to_order_blocks(mainresult, groupList)
            if count > 1:
                # y_position = res.get_Y_position()
                height_after = res.get_height_of_chart_after_double_click(groupList)
                # y_position_after = res.get_Y_position_after_double_click()
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult, 'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, "Chart Height": height, "Chart Height after Double Click": height_after, "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
            else:
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult, 'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, "Chart Height": height, "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
                
            # print(groupList)
            # print(mainresult)
            return returnDict
        else:
            groupList, mainresult, expresult, lowerresult, upperresult, outlierresult, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict = res.do_steps()
            print("END DO STEPS")
            height = res.get_chart_height(groupList)
            fuelConsRes = res.process_fuel_consumption()
            # individual_params = res.individual_parameters()
            count=0
            for i in groupList:
                if i['group_availability_code']%10 == 0:
                    count = count + 1
            if('Multi Axis' in group):
                namesList = res.get_short_names_list_for_multi_axis()
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, 'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, 'Short Names': namesList, "Chart Height": height, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
                # grpList = res.get_generic_group_list()
            variables_list = res.variable_list_according_to_order_blocks(mainresult, groupList)
            if count > 1:
                # y_position = res.get_Y_position()
                height_after = res.get_height_of_chart_after_double_click(groupList)
                # y_position_after = res.get_Y_position_after_double_click()
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, 'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, "Chart Height": height, "Chart Height after Double Click": height_after, "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
            else:
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, 'Outlier': outlierresult, "Fuel Consumption": fuelConsRes, "Chart Height": height, "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
                
            # print(groupList)
            # print(mainresult)
            return returnDict
    else:
        res = TrendsExtractor(ship_imo, group, duration, individual_parameters, include_outliers, compare, anomalies)
        if 'Lastyear' in duration:
            groupList, mainresult, expresult, lowerresult, upperresult, lyexpresult, lylowerresult, lyupperresult, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict = res.do_steps()
            height = res.get_chart_height(groupList)
            fuelConsRes = res.process_fuel_consumption()
            # individual_params = res.individual_parameters()
            count=0
            for i in groupList:
                if i['group_availability_code']%10 == 0:
                    count = count + 1
            if('Multi Axis' in group):
                namesList = res.get_short_names_list_for_multi_axis()
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult, "Fuel Consumption": fuelConsRes, 'Short Names': namesList, "Chart Height": height, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
                # grpList = res.get_generic_group_list()
            variables_list = res.variable_list_according_to_order_blocks(mainresult, groupList)
            if count > 1:
                # y_position = res.get_Y_position()
                height_after = res.get_height_of_chart_after_double_click(groupList)
                # y_position_after = res.get_Y_position_after_double_click()
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult, "Fuel Consumption": fuelConsRes, "Chart Height": height, "Chart Height after Double Click": height_after, "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
            else:
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "LY_Expected": lyexpresult, "LY_Lower": lylowerresult, "LY_Upper": lyupperresult, "Fuel Consumption": fuelConsRes, "Chart Height": height, "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
                
            # print(groupList)
            # print(mainresult)
            return returnDict
        else:
            groupList, mainresult, expresult, lowerresult, upperresult, loaded_ballast_list, spedict, spelimitdict, t2dict, t2limitdict = res.do_steps()
            height = res.get_chart_height(groupList)
            fuelConsRes = res.process_fuel_consumption()
            # individual_params = res.individual_parameters()
            count=0
            for i in groupList:
                if i['group_availability_code']%10 == 0:
                    count = count + 1
            if('Multi Axis' in group):
                namesList = res.get_short_names_list_for_multi_axis()
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "Fuel Consumption": fuelConsRes, 'Short Names': namesList, "Chart Height": height, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
                # grpList = res.get_generic_group_list()
            variables_list = res.variable_list_according_to_order_blocks(mainresult, groupList)
            if count > 1:
                # y_position = res.get_Y_position()
                height_after = res.get_height_of_chart_after_double_click(groupList)
                # y_position_after = res.get_Y_position_after_double_click()
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "Fuel Consumption": fuelConsRes, "Chart Height": height, "Chart Height after Double Click": height_after, "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
            else:
                returnDict = {"group": groupList, "mainres": mainresult, "Expected": expresult, "Lower": lowerresult, "Upper": upperresult, "Fuel Consumption": fuelConsRes, "Chart Height": height, "Variables List": variables_list, 'Ballast or Loaded': loaded_ballast_list, 'SPE': spedict, 'SPE Limit': spelimitdict, 'T2': t2dict, 'T2 Limit': t2limitdict}
                
            # print(groupList)
            # print(mainresult)
            return returnDict

@application.route('/trends/individual', methods=['GET'])
@cross_origin()
def getIndividualParameters():
    configuration = Configurator(9591301)
    ship_configs = configuration.get_ship_configs()
    options = configuration.get_individual_parameters(index=True)
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
    res = TrendsExtractor(ship_imo, group, duration, individual_parameters, include_outliers, compare, anomalies)
    groupsList = res.get_groups_selection_for_table_component()
    returnDict = {"Groups": groupsList}
    return returnDict

@application.route('/dailyreport', methods=['GET'])
@cross_origin()
def getDailyReport():
    args = request.args
    ship_imo = args['ship_imo']
    res = DailyReportExtractor(ship_imo)
    category_list, category_data, column_headers, subcategory_data, dateList, latestRes, anomalyList, static_data, charter_party_values = res.process_data()
    
    return {"Category List": category_list, "Category Dictionary": category_data, 'Sub Category Dictionary': subcategory_data, 'Dates': dateList, "Latest Result": latestRes, 'Daily Report Column Headers': column_headers, 'Anomaly List': anomalyList, 'Static Data': static_data, 'Charter Party Values': charter_party_values}

@application.route('/dailyreport/historical', methods=['GET'])
@cross_origin()
def getSpecificDailyReport():
    args = request.args
    ship_imo = args['ship_imo']
    dateString = args['dateString']
    res = DailyReportExtractor(ship_imo, dateString)

    category_list, category_data, column_headers, subcategory_data, dateList, latestRes, anomalyList, static_data, charter_party_values = res.process_data()
    # historicalresult = res.read_data_for_specific_date(dateString)

    return {"Category List": category_list, "Category Dictionary": category_data, 'Sub Category Dictionary': subcategory_data, 'Dates': dateList, "Latest Result": latestRes, 'Daily Report Column Headers': column_headers, 'Anomaly List': anomalyList, 'Static Data': static_data, 'Charter Party Values': charter_party_values}

@application.route('/interactive', methods=['GET'])
@cross_origin()
def getInteractive():
    args = request.args
    ship_imo = args['ship_imo']
    X = args['X']
    Y = args['Y']
    duration = args['duration']
    stringConstantNames = args['constantNames']
    stringConstantValues = args['constantValues']
    listConstantNames = stringConstantNames.replace(']', '').replace('[', '').split(',')
    listConstantValues = stringConstantValues.replace(']', '').replace('[', '').split(',')
    values = [int(i) for i in listConstantValues]
    other_X = {}
    for i in range(len(listConstantNames)):
        if listConstantNames[i] != "None":
            other_X[listConstantNames[i]] = values[i]
        else:
            continue
    # print(other_X)
    if args.get('Z') != None:
        Z = args['Z']
        res = InteractiveExtractor(ship_imo, X, Y, duration, Z, **other_X)
        x_name, y_name, z_name, result = res.read_data()
        return {"X_name": x_name, "Y_name": y_name, "Z_name": z_name, "Result": result}
    else:
        res = InteractiveExtractor(ship_imo, X, Y, duration, **other_X)
        x_name, y_name, result = res.read_data()
        return {"X_name": x_name, "Y_name": y_name, "Result": result}

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
    res = OverviewExtractor()
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

@application.route('/sendBackend', methods=['POST'])
@cross_origin()
def sendBackend():
    # if request.method == 'POST':
        # if 'file' not in request.files:
        #     flash('No file part')
        #     return redirect(request.url)
        # else:
        # file = request.files['file']
        # file2 = request.files['file2']
        # print("NAME",file1.filename, file2.filename)
        # if file1.filename == '' or file2.filename == '':
        #     flash('One or more files not selected')
        #     return redirect(request.url)
        # if file1 and allowed_file(file1.filename) and file2 and allowed_file(file2.filename):
        #     filename1 = secure_filename(file1.filename)
        #     filename2 = secure_filename(file2.filename)
        print(request.get_json())
        return {'File': request.get_json()}
    # return '''
    #     <!doctype html>
    #     <title>Upload new File</title>
    #     <h1>Upload new File</h1>
    #     <form method=post enctype=multipart/form-data>
    #     <input type=file name=file>
    #     <input type=submit value=Upload>
    #     </form>
    # '''

if __name__ == '__main__':
    application.run(debug=True)
