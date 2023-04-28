import configparser
import os.path

import numpy as np
import pandas as pd

config_ready = True
config_file = "./Config/config.ini"

source_input_file, target_input_file = "", ""
source_key, target_key = [], []
source_delimiter, target_delimiter = "", ""
source_columns_excluded, target_columns_excluded = [], []
mode, column_feeder_file = "", ""
html_report, extended_report = "", ""


def compare_csv(s_input_file, t_input_file, s_key, t_key, s_delimiter, t_delimiter, s_columns_excluded,
                t_columns_excluded, report, ext_report):
    source_data = pd.read_csv(s_input_file, index_col=s_key, sep=s_delimiter)
    target_data = pd.read_csv(t_input_file, index_col=t_key, sep=t_delimiter)
    source_data = source_data.replace(np.nan, "")
    target_data = target_data.replace(np.nan, "")
    source_record_count = len(source_data.axes[0])
    target_record_count = len(target_data.axes[0])
    matched_records = 0
    unmatched_records = 0
    records_in_source_only = 0
    records_in_target_only = 0
    s_columns = []
    t_columns = []
    ext_report = open("./Output/"+ext_report, "w")
    ext_report.write("Key,Column,Source_Value,Target_Value"+"\n")
    for column in source_data.columns:
        if column not in s_columns_excluded:
            s_columns.append(column)
    for column in target_data.columns:
        if column not in t_columns_excluded:
            t_columns.append(column)
    for s_index, s_row in source_data.iterrows():
        t_columns_temp = t_columns.copy()
        records_matched = True
        source_only = False
        for column in s_columns:
            if s_index in target_data.index:
                if column in t_columns_temp:
                    t_row = target_data.loc[s_index]
                    if s_row[column] != t_row[column]:
                        records_matched = False
                        ext_report.write(str(s_index) + "," + str(column) + "," + str(s_row[column]) + "," +
                                         str(t_row[column])+"\n")
                    t_columns_temp.remove(column)
                else:
                    records_matched = False
                    ext_report.write(str(s_index)+","+str(column)+","+str(s_row[column])+","+"\n")
            else:
                source_only = True
                ext_report.write(str(s_index)+","+str(column)+","+str(s_row[column])+","+"\n")
        if not source_only:
            for column in t_columns_temp:
                t_row = target_data.loc[s_index]
                records_matched = False
                ext_report.write(str(s_index) + "," + str(column) + "," + "," + str(t_row[column]) + "\n")
        if s_index in target_data.index:
            target_data = target_data.drop(s_index)
        if source_only:
            records_in_source_only = records_in_source_only + 1
        else:
            if records_matched:
                matched_records = matched_records + 1
            else:
                unmatched_records = unmatched_records + 1
    for t_index, t_row in target_data.iterrows():
        records_in_target_only = records_in_target_only + 1
        for column in t_columns:
            ext_report.write(str(t_index) + "," + str(column) + "," + "," + str(t_row[column]) + "\n")
    ext_report.close()
    print("source_record_count " + str(source_record_count))
    print("target_record_count " + str(target_record_count))
    print("matched_records " + str(matched_records))
    print("unmatched_records " + str(unmatched_records))
    print("records_in_source_only " + str(records_in_source_only))
    print("records_in_target_only " + str(records_in_target_only))
    create_html_report(source_record_count, target_record_count, matched_records, unmatched_records,
                       records_in_source_only, records_in_target_only, report)


def create_html_report(s_record_count, t_record_count, matched_count, unmatched_count, s_only_count, t_only_count,
                       h_report):
    """To be implemented"""


def csv_compare_with_feeder():
    """To be implemented"""


def csv_compare_with_feeder_only():
    """To be implemented"""


def line_compare():
    """To be implemented"""


if os.path.exists(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    if "input" in config.sections():
        try:
            source_input_file = config["input"]["source_input_file"]
            if source_input_file == "":
                print("source_input_file should not be empty")
                config_ready = False
            else:
                source_input_file = "./Input/" + source_input_file
                if not os.path.exists(source_input_file):
                    config_ready = False
            target_input_file = config["input"]["target_input_file"]
            if target_input_file == "":
                print("target_input_file should not be empty")
                config_ready = False
            else:
                target_input_file = "./Input/" + target_input_file
                if not os.path.exists(target_input_file):
                    config_ready = False
            source_key = config["input"]["source_key"]
            source_key = source_key.split(",")
            if len(source_key) == 0:
                print("source_key should not be empty")
                config_ready = False
            target_key = config["input"]["target_key"]
            target_key = target_key.split(",")
            if len(target_key) == 0:
                print("target_key should not be empty")
                config_ready = False
            source_delimiter = config["input"]["source_delimiter"]
            if source_delimiter == "":
                print("source_delimiter should not be empty")
                config_ready = False
            target_delimiter = config["input"]["target_delimiter"]
            if target_delimiter == "":
                print("target_delimiter should not be empty")
                config_ready = False
            source_columns_excluded = config["input"]["source_columns_excluded"]
            source_columns_excluded = source_columns_excluded.split(",")
            target_columns_excluded = config["input"]["target_columns_excluded"]
            target_columns_excluded = target_columns_excluded.split(",")
            mode = config["input"]["mode"]
            column_feeder_file = config["input"]["column_feeder_file"]
        except KeyError as e:
            print(str(e) + " is not present in the input section")
            config_ready = False
    else:
        print("input section is not found in the config file. Please check!")
        config_ready = False
    if "output" in config.sections():
        try:
            html_report = config["output"]["html_report"]
            extended_report = config["output"]["extended_report"]
        except KeyError as e:
            print(str(e) + " is not present in the output section")
            config_ready = False
    else:
        print("output section is not found in the config file. Please check!")
        config_ready = False
else:
    print("Config file not found. Please check!")
    config_ready = False

if config_ready:
    if mode == "csv_only":
        print("****************Execution Started****************")
        compare_csv(source_input_file, target_input_file, source_key, target_key, source_delimiter, target_delimiter,
                    source_columns_excluded, target_columns_excluded, html_report, extended_report)
        print("****************Execution Completed****************")
    elif mode == "feeder_only":
        csv_compare_with_feeder_only()
    elif mode == "csv_with_feeder":
        csv_compare_with_feeder()
    elif mode == "line_compare":
        line_compare()
    else:
        print("Invalid mode. Please check!")
