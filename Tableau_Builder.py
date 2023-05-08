#!/usr/bin/env python3

# @author Futuhal Arifin Annasri - IBM Security
# This script will prepares table in MSSQL from a database called QRadarLogs to another table called Tableau_XXXX for Tableau data visualization.

import sys
import os
import json
import csv
import pyodbc
import calendar
import datetime as dt
from datetime import datetime, timedelta


def main():
	connection = get_mssql_connection()
	qradar_raw_log_db = 'QRadarLogs'
	date = datetime.now()

	build_daily_report(connection, date)
	build_weekly_report(connection, date)
	build_monthly_report(connection, date)


def build_report(connection, list_date, table_name_prefix, week_setting='automatic'):
	cursor = connection.cursor()
	query_expression = "SELECT name FROM master.dbo.sysdatabases"
	cursor.execute(query_expression)
	db_names = cursor.fetchall()
	
	tables = []
	for name in db_names:
		if 'T_' in name[0][0:2]:
			tables.append(name[0])

	query_expression = """
						USE QRadarLogs
						"""
	cursor.execute(query_expression)

	query_expression = """
						SELECT TABLE_NAME
						FROM INFORMATION_SCHEMA.TABLES
						ORDER BY TABLE_NAME 
						"""
	cursor.execute(query_expression)
	db_names = cursor.fetchall()
    
	table_raw = []
	week_dict = get_week_number_by_order(list_date)

	for name in db_names:
		table_raw.append(name[0])

	for table in tables:

		table_for_union = []
		week_name = ''

		for date in list_date:
			if ('MONTHLY' in table_name_prefix):
				if week_setting == 'automatic':
					week_name = ", '{0}' as 'Week'".format(get_week_number(dt.date(int(date[0:4]), int(date[4:6]), int(date[6:8]))))
				else:
					week_name = ", '{0}' as 'Week'".format(week_dict[date])

			if table + '_' + date in table_raw:
				table_for_union.append('SELECT * {0} FROM [QRadarLogs].[dbo].[{1}]'.format(week_name, table + '_' + date))

		if not table_for_union:
			continue

		query_expression = "USE {0}".format(table)
		cursor.execute(query_expression)

		query_expression = "DROP TABLE IF EXISTS {0}".format(table_name_prefix + table + '_' + list_date[0])
		cursor.execute(query_expression)
		connection.commit()

		query_expression = """
			SELECT *
			INTO  [{0}].[dbo].[{1}]
			FROM (
				{2}
			) as tmp
		""".format(table, table_name_prefix + table + '_' + list_date[0], ' UNION ALL '.join(table_for_union))
		cursor.execute(query_expression)
		cursor.commit()

		print('Success created Table ' + table_name_prefix + table + '_' + list_date[0])


def get_week_number_by_order(list_date):
	date_ = {}
	date__ = []
	date_sorted = {}

	for date in list_date:
		date_[date] = dt.date(int(date[0:4]), int(date[4:6]), int(date[6:8]))
		date__.append(dt.date(int(date[0:4]), int(date[4:6]), int(date[6:8])))

	date__ = sorted(date__)

	limiter = 7
	week_number = 1
	counter = 0

	for date in date__:

		date_sorted[date] = 'W' + str(week_number)
		counter = counter + 1

		if counter == limiter:
			counter = 0
			week_number = week_number + 1

	for date in list_date:
		date_[date] = date_sorted[date_[date]]

	return date_


def build_daily_report(connection, date):
	print('Creating Daily Report')
	list_date = []

	# in daily report, we take report from past 3 days
	for i in range(1,4):
		list_date.append(datetime.strftime(date - timedelta(i), '%Y%m%d'))

	table_name_prefix = 'DAILY_'

	build_report(connection, list_date, table_name_prefix)


def build_weekly_report(connection, date):
	list_date = []
	report_date = date - timedelta(1)

	if datetime.strftime(report_date, '%A') in 'Sunday':
		print('Creating Weekly Report')
		# in weekly report, we take report from past 7 days
		for i in range(1, 8):
			list_date.append(datetime.strftime(date - timedelta(i), '%Y%m%d'))

		table_name_prefix = get_week_number(report_date) + '_'
		build_report(connection, list_date, table_name_prefix)
	else:
		print('This is not the right time to create weekly report.')


def build_monthly_report(connection, date):
	list_date = []
	report_date = date - timedelta(1)
	
	if datetime.strftime(report_date, '%A') in 'Sunday' and is_this_last_week_of_month(report_date):
		print('Creating Monthly Report')
		# in monthly report, we take report from past month
		date_diff = get_the_first_day_of_month(report_date) - date
		for i in range(1, abs(date_diff.days) + 1):
			list_date.append(datetime.strftime(date - timedelta(i), '%Y%m%d'))

		table_name_prefix = 'MONTHLY_'
		build_report(connection, list_date, table_name_prefix)
	else:
		print('This is not the right time to create monthly report.')	


def get_week_info(date):
	get_first_date = date.replace(day=1)
	date_range = calendar.monthrange(date.year, date.month)
	date_and_day = [None] * (date_range[1] + 1)
	week = []
	list_week = {}
	counter = 1
	last_three_week = ['Friday', 'Saturday', 'Sunday']

	for i in range(1, date_range[1] + 1):
		date_and_day[i] = date.replace(day=i).strftime('%A')

		week.append(i)
		
		if i == 1 and date_and_day[i] in last_three_week:
			counter = 0 

		if date_and_day[i] in 'Sunday' or i == date_range[1]:
			list_week['W' + str(counter)] = week
			week = []
			counter = counter + 1

	return list_week 


def get_week_number(date):
	list_week = get_week_info(date)
	week_number = 'W'

	for key in list_week:
		if key == 'W0' and date.day in list_week['W0']:
			new_list_week = get_week_info(date.replace(day=1) - timedelta(days=1))
			temp = []
			for n in new_list_week:
				temp.append(n) 

			week_number = temp[len(temp) - 1]
			continue

		if date.day in list_week[key]:
			if len(list_week[key]) < 4:
				key = 'W1'

			week_number = key

	return week_number


def is_this_last_week_of_month(date):
	this_week = get_week_number(date)
	list_week = get_week_info(date)

	temp = []
	for key in list_week:
		if key == 'W5' and len(list_week[key]) < 4:
			continue
		temp.append(key)

	if this_week == 'W5' or (this_week == 'W4' and 'W5' not in temp) or this_week == 'W0':
		return True
	else:
		return False


def get_the_first_day_of_month(date):
	list_week = get_week_info(date)

	if 'W0' in list_week:
		if date.day in list_week['W0']:
			date = date.replace(day=1) - timedelta(days=1)
			list_week = get_week_info(date)

	date_of_week = list_week['W1']

	if len(date_of_week) < 7:
		t = 7 - len(date_of_week)
		date = date.replace(day=date_of_week[0]) - timedelta(t)
	else:
		date = date.replace(day=date_of_week[0])

	return date


def get_mssql_connection():
	connection = pyodbc.connect('driver={ODBC Driver 17 for SQL Server};server=DBHOST;uid=DBUSER;pwd=DBPASSW;ColumnEncryption=Enabled;') #Hardcoded
	
	return connection


if __name__ == "__main__":
	main()
