#!/usr/bin/env python3

# @author Futuhal Arifin Annasri - IBM Security
# This script will export data from QRadar to csv file based on Ariel query.
# By default, the script will get the data for last 1 day.
# It has a dependency to use some script in the module folder.

import sys
import os
import json
import csv
import re
import datetime as dt

from calendar import monthrange
from datetime import datetime, timedelta

sys.path.append(os.path.realpath('modules'))
from arielapiclient import APIClient


def main():
	api_client = APIClient()
	date = datetime.now()

	day_plus_one = datetime.strftime(date, '%Y-%m-%d')
	day = datetime.strftime(date - timedelta(1), '%Y-%m-%d')

	time_period = {}
	time_period['day_plus_one'] = day_plus_one
	time_period['day'] = day

	get_daily_log(api_client, time_period)


def get_daily_log(api_client, time_period):
	day_filename = time_period['day'].replace('-','')
	create_log(get_json_log(api_client, get_tableau_custom_query1(time_period)), 'Tableau_Custom_Query1_{}.csv'.format(day_filename))


def get_json_log(api_client, query_expression):
	response = api_client.create_search(query_expression)
	response_json = json.loads(response.read().decode('utf-8'))
	search_id = response_json['search_id']
	response = api_client.get_search(search_id)
	error = False

	while (response_json['status'] != 'COMPLETED') and not error:
		if (response_json['status'] == 'EXECUTE') | \
				(response_json['status'] == 'SORTING') | \
				(response_json['status'] == 'WAIT'):
			response = api_client.get_search(search_id)
			response_json = json.loads(response.read().decode('utf-8'))
		else:
			print(response_json['status'])
			error = True

	response = api_client.get_search_results(search_id, 'application/json')
	body = response.read().decode('utf-8')
	body_json = json.loads(body)

	return body_json['events']


def get_tableau_query1(time_period):
	query_expression = """
		###YOUR QRADAR AQL QUERY HERE###
		""".format(time_period['day'], time_period['day_plus_one'])

	return query_expression


def create_log(json_data, filename):
	output_path = os.path.realpath('csv4tableau')
	keylist = []

	writer = csv.writer(open(os.path.join(output_path, filename), "a+", newline='', encoding="utf-8"))
	
	if json_data:
		for key in json_data[0]:
			keylist.append(key)

		if os.stat(os.path.join(output_path, filename)).st_size == 0:
			writer.writerow(keylist)

		for record in json_data:
			current_record = []
			for key in keylist:
				final_str = ''
				final_str = re.sub(r'[^a-z A-Z\\/0-9:._-]', '', str(record[key]).rstrip("\n\r"))
				current_record.append(final_str)

			writer.writerow(current_record)
	print("Created {}".format(filename))


if __name__ == "__main__":
	main()
