import pandas as pd

def get_subgroup_names(groupname):
        ''' Returns a dictionary for sub-group names. Group name is key, sub-group name is value.'''
        subgroup_name_file = pd.read_excel("src/processors/config_extractor/Sub Group Names.xlsx", engine="openpyxl")
        subgroup_name_file_dict = subgroup_name_file.to_dict("list")
        subgroup_dict_names = {}
        subgroup_dict_labels = {}

        for key in subgroup_name_file_dict.keys():
            if 'Serial' in key:
                subgroup_dict_labels[key] = subgroup_name_file[key]
            else:
                subgroup_dict_names[key] = subgroup_name_file[key]

        print("LABELS", subgroup_dict_labels)
        print("NAMES", subgroup_dict_names)
        subgroup_names = subgroup_dict_names[groupname]
        index_of_sub_group_column = list(subgroup_dict_names.keys()).index(groupname)
        name_of_corresponding_column = list(subgroup_dict_labels.keys())[index_of_sub_group_column]
        subgroup_labels = subgroup_dict_labels[name_of_corresponding_column]

        subgroup_labels = [int(i) if type(i) == float and pd.isnull(i) == False else i for i in subgroup_labels ]
        subgroup_labels_new = ['Sub-Group ' + str(i) for i in subgroup_labels if pd.isnull(i) == False]
        print(subgroup_labels_new, subgroup_names)
        subgroup_dict = dict(zip(subgroup_labels_new, subgroup_names))

        for key in subgroup_dict.copy():
           if pd.isnull(key) == True:
               del subgroup_dict[key]
        
        return subgroup_dict

new_dict = get_subgroup_names("BASIC")
print(new_dict)
