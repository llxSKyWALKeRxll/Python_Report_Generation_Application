"""
Contains several useful queries for database
"""
from typing import List


def all_unique_store_ids_query():
    """
    query to fetch each store's id
    :return: query in string format
    """
    return "select distinct(store_id) from store_status"


def fetch_all_working_hours_for_store_ids_query(store_ids: List[int]):
    """
    query to fetch the working hours for each store passed in the param
    :param store_ids: store ids for which the working hours are to be extracted
    :return: query in string format
    """
    return f'select store_id, day, start_time_local, end_time_local from menu_hours where store_id in ({", ".join(map(str, store_ids))})'


def fetch_active_inactive_status_for_stores_query(store_ids: List[int], date: str, all_timestamps: List[tuple]):
    finalQuery = "SELECT store_id, " \
                 "COUNT(CASE WHEN status = 'active' THEN 1 END) AS active_count, " \
                 "COUNT(CASE WHEN status != 'active' THEN 1 END) AS inactive_count " \
                 "FROM store_status "
    finalQuery += f"WHERE store_id in ({', '.join(map(str, store_ids))}) "
    if all_timestamps:
        finalQuery += "AND ( "
        for ind, timestamp in enumerate(all_timestamps):
            if len(timestamp) >= 2:
                if ind == 0:
                    finalQuery += f"timestamp_utc BETWEEN '{date} {timestamp[0]}' AND '{date} {timestamp[1]}' "
                else:
                    finalQuery += f"OR timestamp_utc BETWEEN '{date} {timestamp[0]}' AND '{date} {timestamp[1]}' "
        finalQuery += ") "
    finalQuery += "GROUP BY store_id"
    return finalQuery


def fetch_all_status_query():
    """
    query to fetch each store's status at all polling hours
    :return: string query
    """
    return f'select store_id, timestamp_utc, status from store_status order by timestamp_utc asc'


def fetch_report_exists_query(report_id):
    """
    query to check if a report with the given report id exists or not
    :param report_id: report id to be checked
    :return: string query
    """
    return f"SELECT EXISTS(SELECT 1 FROM report_id_mapping WHERE report_id = '{report_id}');"


def fetch_find_name_query(report_id):
    """
    query to fetch file name for the given report id
    :param report_id: report id for which the file name is to be fetched
    :return: string query
    """
    return f"select filename from report_id_mapping where report_id = '{report_id}';"


def fetch_check_report_status_query(report_id):
    """
    query to check report status for the given report id
    :param report_id: report id for which the status is to be checked
    :return: string query
    """
    return f"SELECT report_status FROM report_generation_status WHERE report_id = '{report_id}'"


def fetch_insert_report_id_map_query(report_id, filename):
    """
    query to insert record into report_id_mapping table
    :param report_id: report id to be added
    :param filename: filename to be added
    :return: query string
    """
    return f"INSERT INTO report_id_mapping (report_id, filename) " \
           f"VALUES ('{report_id}', '{filename}')"


def fetch_check_report_id_exists_query(random_string):
    return f"SELECT EXISTS(SELECT 1 FROM report_id_mapping WHERE report_id = '{random_string}');"


def fetch_insert_report_generation_status_query(report_id, status):
    return f"INSERT INTO report_generation_status (report_id, report_status) " \
           f"VALUES ('{report_id}', '{status}')"


def fetch_updated_report_generation_status_query(report_id, status):
    return f"UPDATE report_generation_status " \
           f"SET report_status = '{status}' " \
           f"WHERE report_id = '{report_id}'"
