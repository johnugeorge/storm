from pandas import *
from collections import Counter
import storm
import json

class CognitiveBolt(storm.BasicBolt):
    def process(self, tup):
        message = json.loads(tup.values[0])
        result = {"exp_id":message['exp_id']}
        max_results = 10
        input_data = DataFrame
        feature_names = None
        feature_types = None
        output_data = None
        for data in message['graph']:
            component_id = int(data)
            op = json.loads(message['components'][data])[0]['fields']
            if op['function_type'] == 'Create':
                if op['function_arg'] == 'Table':
                    if op['function_subtype'] == 'Input':
                        filename = op['function_subtype_arg']
                        #input_data = read_csv(filename)
                        d = {'one' : [1., 2., 3., 4.], 'two' : [4., 3., 2., 1.]}
                        input_data = DataFrame(d)
                        feature_names = input_data.columns
                if op['function_arg'] == 'Row':
                    if op['function_subtype'] == 'Row':
                        row_values = json.loads(op['function_subtype_arg'])
                        input_data.loc[len(input_data)+1]= row_values
                if op['function_arg'] == 'Model':
                    if op['function_subtype'] == 'Train-Test':
                        params = json.loads(op['function_subtype_arg'])
                        train_data_percentage = int(params["train_data_percentage"])
                        target_column = int(params["target_column"])
                        model_type = op['function_arg_id']
                        #print model_type, train_data_percentage,target_column
                        target_feature = feature_names[target_column]
                        actual_target_column = input_data.columns.get_loc(target_feature)
                        input_feature_columns = range(len(input_data.columns))
                        input_feature_columns.remove(actual_target_column)
                        input_features = input_data.columns[input_feature_columns]
                        classifier = Classifier(input_data, model_type, train_data_percentage, input_features ,target_feature)
                        output_data = classifier.learn()

            if op['function_type'] == 'Update' :
                if op['function_arg'] == 'Table':
                    if op['function_subtype'] == 'Metadata':
                        feature_types = json.loads(op['function_subtype_arg'])
                        #print "Feature Names" ,feature_names, " Feature_types ", feature_types

                if op['function_arg'] == 'Column':
                    if op['function_subtype'] == 'Add':
                        constant_value = float(op['function_subtype_arg'])
                        column_id = float(op['function_arg_id'])
                        column_name = feature_names[column_id]
                        if column_name not in input_data:
                            #print "Column name ",column_name, " not present. Skipping"
                            continue#throw error in module status
                        if input_data[column_name].dtype == 'object':
                            #print "Column name ",column_name, " is not integer/float. Skipping"
                            continue#throw error in module status
                        input_data[column_name] = input_data[column_name] + constant_value
                    if op['function_subtype'] == 'Sub':
                        constant_value = float(op['function_subtype_arg'])
                        column_id = float(op['function_arg_id'] )
                        column_name = feature_names[column_id]
                        if column_name not in input_data:
                            #print "Column name ",column_name, " not present. Skipping"
                            continue#throw error in module status
                        if input_data[column_name].dtype == 'object':
                            #print "Column name ",column_name, " is not integer/float. Skipping"
                            continue#throw error in module status
                        input_data[column_name] = input_data[column_name] - constant_value
                    if op['function_subtype'] == 'Mult':
                        constant_value = float(op['function_subtype_arg'])
                        column_id = float(op['function_arg_id'] )
                        column_name = feature_names[column_id]
                        if column_name not in input_data:
                            #print "Column name ",column_name, " not present. Skipping"
                            continue#throw error in module status
                        if input_data[column_name].dtype == 'object':
                            #print "Column name ",column_name, " is not integer/float. Skipping"
                            continue#throw error in module status
                        input_data[column_name] = input_data[column_name] * constant_value
                    if op['function_subtype'] == 'Div':
                        constant_value = float(op['function_subtype_arg'])
                        column_id = float(op['function_arg_id'] )
                        column_name = feature_names[column_id]
                        if column_name not in input_data:
                            #print "Column name ",column_name, " not present. Skipping"
                            continue#throw error in module status
                        if input_data[column_name].dtype == 'object':
                            #print "Column name ",column_name, " is not integer/float. Skipping"
                            continue#throw error in module status
                        input_data[column_name] = input_data[column_name] / constant_value
                    if op['function_subtype'] == 'Normalize':
                        column_id = float(op['function_arg_id'] )
                        column_name = feature_names[column_id]
                        sum_array = input_data.sum(axis=0)
                        if column_name not in sum_array:
                            #print "Column name ",column_name, " not present. Skipping"
                            continue#throw error in module status
                        normalization_value = sum_array[column_name]
                        input_data[column_name] = input_data[column_name] / normalization_value
            if op['function_type'] == 'Filter' :
                if op['function_arg'] == 'Table':
                    if op['function_subtype'] == 'Project':
                        column_id_list = json.loads(op['function_arg_id'] )
                        excluded_columns = range(len(feature_names))
                        for elem in column_id_list:#Bug: Calling Projection twice will break indexing logic
                            excluded_columns.remove(elem)
                        excluded_columns = [ x for x in excluded_columns if feature_names[x] in input_data ]
                        #print "Excluded columns ", excluded_columns
                        if excluded_columns:
                            input_data = input_data.drop(feature_names[excluded_columns], axis=1)
                    if op['function_subtype'] == 'RemoveDup':
                        column_id_list = json.loads(op['function_arg_id'] )
                        column_name_list = []
                        for elem in column_id_list:
                            column_name = feature_names[elem]
                            if column_name not in input_data:
                                #print "Column name ",column_name, " not present. Skipping"
                                continue#throw error in module status
                            column_name_list.append(column_name)
                        if column_name_list:
                            input_data = input_data.drop_duplicates(subset = column_name_list)
                    if op['function_subtype'] == 'RemoveMissing':
                        if op['function_subtype_arg'] == 'Replace_mean':
                            input_data = input_data.fillna(input_data.mean().round(2))
                        if op['function_subtype_arg'] == 'Replace_median':
                            input_data = input_data.fillna(input_data.median().round(2))
                        if op['function_subtype_arg'] == 'Replace_mode':
                            input_data = input_data.fillna(input_data.mode())
                        if op['function_subtype_arg'] == 'Drop_row':
                            input_data = input_data.dropna(axis = 0)
            #print "Data"
            #print input_data
            #print "Data Type"
            #print input_data.dtypes
            if component_id == message['result']:
                #print "End component reached"
                result["feature_names"] = list(input_data.columns)
                if feature_types is not None:
                    result["feature_types"] = feature_types
                #result["data"] = input_data[:max_results].to_json()
                result["data"]= []
                result_length = min (len(input_data), max_results)
                result["data"]= []
                result_length = min (len(input_data), max_results)
                for i in range(result_length):
                    tmp = []
                    for col in input_data.columns:
                        if json.dumps(input_data[col][i]) == 'NaN':
                            tmp.append('')
                        else:
                            tmp.append(input_data[col][i])
                    result["data"].append(tmp)
                result["graph_data"] = []
                for name in list(input_data.columns):
                    top_uniques = Counter(list(input_data[name])).most_common(4)
                    col_names= []
                    unique_count =[]
                    for val in top_uniques:
                        if json.dumps(val[0]) == 'NaN':
                            continue
                        col_names.append(val[0])
                        unique_count.append(val[1])
                    tmp = [col_names, unique_count]
                    result["graph_data"].append(tmp)
                    #tmp = [["a","b","c","d"],[1,1,5,200]]
                    #result["graph_data"].append(tmp)
                if output_data is not None:
                    result["output"] = output_data
                result["missing_values"] = list(input_data.isnull().sum().values)
                mean = input_data.mean().round(2)
                median = input_data.median().round(2)
                result["mean"] = []
                result["median"] = []
                for elem in input_data.columns:
                    if elem in mean:
                        result["mean"].append(mean[elem])
                    else:
                        result["mean"].append('')
                    if elem in median:
                        result["median"].append(median[elem])
                    else:
                        result["median"].append('')
                result["unique_values"] = []
                for elem in input_data.columns:
                    result["unique_values"].append(input_data[elem].nunique())
                result["min"] = []
                result["max"] = []
                result["std"] = []
                result["25_quartile"] =[]
                result["50_quartile"] =[]
                result["75_quartile"] =[]
                metric_val = input_data.describe()
                for elem in input_data.columns:
                    if elem in metric_val:
                        val = metric_val[elem].round(2)
                        result["min"].append(val["min"])
                        result["max"].append(val["max"])
                        result["std"].append(val["std"])
                        result["25_quartile"].append(val["25%"])
                        result["50_quartile"].append(val["50%"])
                        result["75_quartile"].append(val["75%"])
                    else:
                        result["min"].append('')
                        result["max"].append('')
                        result["std"].append('')
                        result["25_quartile"].append('')
                        result["50_quartile"].append('')
                        result["75_quartile"].append('')
                result["total_rows"] = input_data.shape[0]
                result["total_columns"] = input_data.shape[1]
                storm.emit([json.dumps(result)])

CognitiveBolt().run()
