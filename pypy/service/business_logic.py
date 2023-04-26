"""
Implements the business/required logic in order to generate the reports for the stores
"""

import csv
import os
import secrets
import string
from datetime import datetime

from dtos import StoreWorkingHoursDto
from queries import db_queries
from service import db_service


def fetch_all_unique_store_ids():
    """
    fetches the store ids for every store
    :return: a list consisting of each store's id
    """
    return db_service.execute_any_query(db_queries.all_unique_store_ids_query())


def get_working_hours_for_each_store(store_ids):
    """
    fetches the working our for each store passed in the param
    :param store_ids: store ids for which the working hours are to be extracted
    :return: list consisting of working hours for the store_ids
    """
    return db_service.execute_any_query(db_queries.fetch_all_working_hours_for_store_ids_query(store_ids))


def get_store_id_with_working_hours_detail_list():
    """
    returns a map which consists of objects of type StoreDetails,
    where each object contains the store_id, and it's working hours for each separate day (Monday, Tuesday, & so on...)
    :return: Map[StoreWorkingHoursDto] map consisting of StoreWorkingHoursDto objects which are mapped to the store_ids
    """
    storeIdsWithWorkingHoursObjectsList = []
    allStoreIds = [storeId[0] for storeId in fetch_all_unique_store_ids()]
    allStoreIdsMap = {storeId: False for storeId in allStoreIds}
    storeIdsMapWithObjectState = {}
    workingHoursRaw = get_working_hours_for_each_store(allStoreIds)
    for workingHour in workingHoursRaw:
        if workingHour:
            allStoreIdsMap[workingHour[0]] = True
            currStoreDetails = StoreWorkingHoursDto.StoreWorkingHours(workingHour[0]) if workingHour[
                                                                                     0] not in storeIdsMapWithObjectState else \
                storeIdsMapWithObjectState[workingHour[0]]
            start_time_raw = workingHour[2]
            end_time_raw = workingHour[3]
            start_time_raw_str = str(start_time_raw)
            hh, mm, ss = start_time_raw_str.split(":")
            start_time = f"{hh.zfill(2)}:{mm.zfill(2)}:{ss.zfill(2)}"

            end_time_raw_str = str(end_time_raw)
            hh, mm, ss = end_time_raw_str.split(":")
            end_time = f"{hh.zfill(2)}:{mm.zfill(2)}:{ss.zfill(2)}"
            if workingHour[1] == 0:
                start_time, end_time = f"2023-01-23 {start_time}", f"2023-01-23 {end_time}"
                currStoreDetails.set_working_hours("Monday", (start_time, end_time))
            elif workingHour[1] == 1:
                start_time, end_time = f"2023-01-24 {start_time}", f"2023-01-24 {end_time}"
                currStoreDetails.set_working_hours("Tuesday", (start_time, end_time))
            elif workingHour[1] == 2:
                start_time_1, end_time_1 = f"2023-01-18 {start_time}", f"2023-01-18 {end_time}"
                currStoreDetails.set_working_hours("Wednesday", (start_time_1, end_time_1))
                start_time_2, end_time_2 = f"2023-01-25 {start_time}", f"2023-01-25 {end_time}"
                currStoreDetails.set_working_hours("Wednesday", (start_time_2, end_time_2))
            elif workingHour[1] == 3:
                start_time, end_time = f"2023-01-19 {start_time}", f"2023-01-19 {end_time}"
                currStoreDetails.set_working_hours("Thursday", (start_time, end_time))
            elif workingHour[1] == 4:
                start_time, end_time = f"2023-01-20 {start_time}", f"2023-01-20 {end_time}"
                currStoreDetails.set_working_hours("Friday", (start_time, end_time))
            elif workingHour[1] == 5:
                start_time, end_time = f"2023-01-21 {start_time}", f"2023-01-21 {end_time}"
                currStoreDetails.set_working_hours("Saturday", (start_time, end_time))
            elif workingHour[1] == 6:
                start_time, end_time = f"2023-01-22 {start_time}", f"2023-01-22 {end_time}"
                currStoreDetails.set_working_hours("Sunday", (start_time, end_time))
            storeIdsMapWithObjectState[workingHour[0]] = currStoreDetails
    for k, v in storeIdsMapWithObjectState.items():
        if not v.get_monday_working_hours():
            v.set_working_hours("Monday", ("2023-01-23 00:00:00", "2023-01-23 23:59:59"))
        if not v.get_tuesday_working_hours():
            v.set_working_hours("Tuesday", ("2023-01-24 00:00:00", "2023-01-24 23:59:59"))
        if not v.get_wednesday_working_hours():
            v.set_working_hours("Wednesday", ("2023-01-18 00:00:00", "2023-01-18 23:59:59"))
            v.set_working_hours("Wednesday", ("2023-01-25 00:00:00", "2023-01-25 23:59:59"))
        if not v.get_thursday_working_hours():
            v.set_working_hours("Thursday", ("2023-01-19 00:00:00", "2023-01-19 23:59:59"))
        if not v.get_friday_working_hours():
            v.set_working_hours("Friday", ("2023-01-20 00:00:00", "2023-01-20 23:59:59"))
        if not v.get_saturday_working_hours():
            v.set_working_hours("Saturday", ("2023-01-21 00:00:00", "2023-01-21 23:59:59"))
        if not v.get_sunday_working_hours():
            v.set_working_hours("Sunday", ("2023-01-22 00:00:00", "2023-01-22 23:59:59"))
        storeIdsWithWorkingHoursObjectsList.append(v)
    for k, v in allStoreIdsMap.items():
        if not v:
            storeWith24HrsOpenHours = StoreWorkingHoursDto.StoreWorkingHours(k)
            storeWith24HrsOpenHours.set_working_hours("Monday", ("2023-01-23 00:00:00", "2023-01-23 23:59:59"))
            storeWith24HrsOpenHours.set_working_hours("Tuesday", ("2023-01-24 00:00:00", "2023-01-24 23:59:59"))
            storeWith24HrsOpenHours.set_working_hours("Wednesday", ("2023-01-18 00:00:00", "2023-01-18 23:59:59"))
            storeWith24HrsOpenHours.set_working_hours("Wednesday", ("2023-01-25 00:00:00", "2023-01-25 23:59:59"))
            storeWith24HrsOpenHours.set_working_hours("Thursday", ("2023-01-19 00:00:00", "2023-01-19 23:59:59"))
            storeWith24HrsOpenHours.set_working_hours("Friday", ("2023-01-20 00:00:00", "2023-01-20 23:59:59"))
            storeWith24HrsOpenHours.set_working_hours("Saturday", ("2023-01-21 00:00:00", "2023-01-21 23:59:59"))
            storeWith24HrsOpenHours.set_working_hours("Sunday", ("2023-01-22 00:00:00", "2023-01-22 23:59:59"))
            storeIdsMapWithObjectState[k] = storeWith24HrsOpenHours
    return storeIdsMapWithObjectState


def fetch_data_all_stores(allStoreDetails):
    """
    generates a dictionary which consists of details of each store in which key -> store_id and the keys are
    mapped to values with the following structure:
    each value is a dict itself, which has the stated keys -> 'store_id', 'uptime_last_week(in hours)', 'downtime_last_week(in hours)',
    'uptime_last_day(in hours)', 'downtime_last_day(in hours)', 'uptime_last_hour(in minutes)', 'downtime_last_hour(in minutes)'
    :param allStoreDetails: Map[StoreWorkingHoursDto] map consisting of StoreWorkingHoursDto objects which are mapped to the store_ids
    :return: dictionary containing the keys stated above for each store
    """
    timeAndActiveList = db_service.execute_any_query(db_queries.fetch_all_status_query())

    # current time is taken as the max timestamp from the first table (store_status)
    currTime = datetime(2023, 1, 25, 18, 14, 22)
    currDay = int(currTime.strftime("%d"))

    allBusinessHoursForStore = {}

    for curStatus in timeAndActiveList:
        detailsForWorkingHours = allStoreDetails[curStatus[0]]
        working_hours = detailsForWorkingHours.get_working_hours_all()
        day_of_week = curStatus[1].strftime('%A')
        dt_obj = curStatus[1]
        for start_time, end_time in working_hours[day_of_week]:
            start_obj = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            end_obj = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
            if start_obj <= dt_obj <= end_obj:
                if detailsForWorkingHours.get_store_id() not in allBusinessHoursForStore:
                    allBusinessHoursForStore[detailsForWorkingHours.get_store_id()] = []
                allBusinessHoursForStore[detailsForWorkingHours.get_store_id()].append([curStatus[1], curStatus[2]])
    finalRes = []
    for curStoreId, workingHoursTimeList in allBusinessHoursForStore.items():
        storeWorkingHoursDtoObj = allStoreDetails[curStoreId]
        allWorkingHours = storeWorkingHoursDtoObj.get_working_hours_all()
        totalActiveHoursForWeek = 0
        totalInactiveHoursForWeek = 0
        totalActiveHoursForDay = 0
        totalInactiveHoursForDay = 0
        totalActiveHoursForLastHour = 0
        totalInactiveHoursForLastHour = 0
        lastTime = None
        for workingHours in workingHoursTimeList:
            pollTime = workingHours[0]
            numberDay = int(pollTime.strftime("%d"))
            day = pollTime.strftime('%A')
            dayWorkingHours = allWorkingHours[day]
            for ddt in dayWorkingHours:
                start_time = datetime.strptime(ddt[0], '%Y-%m-%d %H:%M:%S')
                end_time = datetime.strptime(ddt[1], '%Y-%m-%d %H:%M:%S')
                if start_time <= pollTime <= end_time:
                    if not lastTime or (lastTime and lastTime < start_time):
                        lastTime = start_time
                        break
            if workingHours[1] == 'active':
                if currDay - numberDay <= 7:
                    totalActiveHoursForWeek += ((pollTime - lastTime).total_seconds() / 3600)
                    if currDay - numberDay == 1:
                        totalActiveHoursForDay += ((pollTime - lastTime).total_seconds() / 3600)
                    if currDay == numberDay:
                        if (currTime - pollTime).total_seconds() <= 3600:
                            totalActiveHoursForLastHour += ((pollTime - lastTime).total_seconds() / 3600)
            elif workingHours[1] == 'inactive':
                if currDay - numberDay <= 7:
                    totalInactiveHoursForWeek += ((pollTime - lastTime).total_seconds() / 3600)
                    if currDay - numberDay == 1:
                        totalInactiveHoursForDay += ((pollTime - lastTime).total_seconds() / 3600)
                    if currDay == numberDay:
                        if (currTime - pollTime).total_seconds() <= 3600:
                            totalInactiveHoursForLastHour += ((pollTime - lastTime).total_seconds() / 3600)
            lastTime = pollTime
        finalRes.append({'store_id': curStoreId,
                         'uptime_last_week(in hours)': min(168, totalActiveHoursForWeek),
                         'downtime_last_week(in hours)': min(168, totalInactiveHoursForWeek),
                         'uptime_last_day(in hours)': min(24, totalActiveHoursForDay),
                         'downtime_last_day(in hours)': min(24, totalInactiveHoursForDay),
                         'uptime_last_hour(in minutes)': min(60, totalActiveHoursForLastHour * 60),
                         'downtime_last_hour(in minutes)': min(60, totalInactiveHoursForLastHour * 60)})
    return finalRes


def generate_csv_file(all_data_for_stores, filename, report_id):
    """
    generates the csv file for the given filename & report id
    :param all_data_for_stores: the data that is to be written into the csv file (list format consisting of dicts)
    :param filename: file name of the csv
    :param report_id: report id of the csv file
    :return: None
    """
    fields = [
        'store_id', 'uptime_last_week(in hours)', 'downtime_last_week(in hours)', 'uptime_last_day(in hours)',
        'downtime_last_day(in hours)', 'uptime_last_hour(in minutes)', 'downtime_last_hour(in minutes)'
    ]
    with open(f"{os.path.dirname(os.path.realpath(__file__))}/../csv_files/{filename}", 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_data_for_stores)
    db_service.execute_any_query(db_queries.fetch_insert_report_id_map_query(report_id, filename))


def generate_and_verify_filename():
    """
    generates and verifies a unique filename for the csv file
    :return: filename in string format
    """
    for _ in range(5):
        alphabet = string.ascii_letters + string.digits
        random_string = ''.join(secrets.choice(alphabet) for i in range(50))
        res = db_service.execute_any_query(db_queries.fetch_check_report_id_exists_query(random_string))
        if not res[0][0]:
            #     key is unique and doesn't already exist in the database
            return f"report_{random_string}.csv", random_string


def generate_csv_file_and_map_to_db(filename, report_id):
    """
    calls all the relevant functions in order to generate a csv file
    :param filename: name of the csv file to be generated
    :param report_id: report id of the csv file
    :return: None
    """
    allStoreDetails = get_store_id_with_working_hours_detail_list()
    all_data_for_stores = fetch_data_all_stores(allStoreDetails)
    generate_csv_file(all_data_for_stores, filename, report_id)
    report_generation_status(report_id, "COMPLETE")


def report_generation_status(report_id, status):
    """
    creates or updates the database for the given report id along with its status
    :param report_id: report id for which the data needs to be created/updated
    :param status: status of the report
    :return: None
    """
    if status == 'RUNNING':
        insert_query = db_queries.fetch_insert_report_generation_status_query(report_id, status)
        db_service.execute_any_query(insert_query)
    elif status == 'COMPLETE':
        update_query = db_queries.fetch_updated_report_generation_status_query(report_id, status)
        db_service.execute_any_query(update_query)


def find_csv_file_by_report_id(report_id):
    """
    finds the csv file for the given report id
    :param report_id: report id for which the csv files needs to be fetched
    :return: location (string format) of the file if it exists, 'RUNNING' string if the report generation is in progress, else None
    """
    check_report_status_query = db_queries.fetch_check_report_status_query(report_id)
    report_status = db_service.execute_any_query(check_report_status_query)
    if not report_status:
        return 'INVALID'
    if report_status[0][0] and report_status[0][0] == 'RUNNING':
        return 'RUNNING'
    report_exists_query = db_queries.fetch_report_exists_query(report_id)
    reportExistsCheck = db_service.execute_any_query(report_exists_query)
    if reportExistsCheck[0][0]:
        # report exists for this report id
        if report_status[0][0] and report_status[0][0] == 'COMPLETE':
            find_filename_query = db_queries.fetch_find_name_query(report_id)
            fileExistsCheck = db_service.execute_any_query(find_filename_query)
            if fileExistsCheck[0][0]:
                filename = fileExistsCheck[0][0]
                filepath = os.path.dirname(os.path.realpath(__file__)) + '/../csv_files'
                file_location = os.path.join(filepath, filename)
                if os.path.isfile(file_location):
                    return file_location
                else:
                    return None
    else:
        # invalid report id
        return None
