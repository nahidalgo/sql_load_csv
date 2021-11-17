import csv

import mysql.connector

filename = "fact_monitoring_new.csv"

db = mysql.connector.connect(
    host="localhost",
    user="tcc",
    password="tcc",
    database="tcc"
)

cursor = db.cursor()

def value_or_null(value):
    if (value):
        return value
    else:
        return 'null'

def start():
    data_to_be_saved = []
    for row in getstuff(filename):
        data_to_be_saved.append(row)

        if (len(data_to_be_saved) > 1000):
            save_to_db(data_to_be_saved)
            data_to_be_saved = []


def getstuff(filename):
    with open(filename, "rt", encoding="utf8") as csvfile:
        datareader = csv.reader(csvfile)
        for row in datareader:
            yield next(datareader)

def save_to_db(data_list):
    for data in data_list:
        cursor.execute(f'insert into fact_monitoring (id, property_id, area_id, region_id, scouter_id, start_time, finish_time, start_location_lat, start_location_lng, finish_location_lat, finish_location_lng, monitoring_id, characteristic_id, inspection_id, double_value, string_value, forced_location_lat, forced_location_lng, user_agent, methodology_id, methodology_version, company_id) values ( \"{data[0]}\", \"{data[1]}\", \"{data[2]}\", \"{data[3]}\", \"{data[4]}\",'
                       f'\"{data[5]}\", \"{data[6]}\", {value_or_null(data[7])}, {value_or_null(data[8])}, {value_or_null(data[9])}, {value_or_null(data[10])}, \"{data[11]}\", \"{data[12]}\",'
                       f'\"{data[13]}\", {value_or_null(data[14])}, \"{data[15]}\", {value_or_null(data[16])}, {value_or_null(data[17])}, \"{data[18]}\", \"{data[19]}\",'
                       f'{value_or_null(data[20])}, \"{data[21]}\")')
    db.commit()

start()
#
#
#
#
