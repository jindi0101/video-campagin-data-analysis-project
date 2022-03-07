#!/usr/bin/env python
# -*- coding: utf-8 -*-
#from pip._internal import main
#main(["install", "numpy"])
#main(["install", "pandas"])
#main(["install", "main"])
#main(["install", "tqdm"])




import os
import pandas as pd
import numpy as np
import math
from tqdm import tqdm

import requests
from bs4 import BeautifulSoup
import re

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
from datetime import date, timedelta
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
from video_index import category_index, topic_index
import json

youtube_query_limit = 50
today = date.today() - timedelta(days=9)
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
video_id_file = ''
result_file = './Results/result_' + str(today) + '.csv'

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/Users/wishare-mini2/Desktop/Video campaign code/service_account.json', scope)
client = gspread.authorize(creds)
read_worksheet = '味全alltime where ads show'
read_sheet = '味全alltime where ads show'
#read_worksheet = 'Automatic placements report umeken'
#read_sheet = 'Automatic placements report umeken'
write_worksheet = 'Sheet1'
write_sheet1 = 'Sheet1'
#write_sheet2 = 'Low Quality'


def read_videos_from_sheet():

	sh = client.open(read_worksheet)
	sheet = sh.worksheet(read_sheet)

	channel_url_list = sheet.col_values(2)
	# campaign_list = sheet.col_values(5)
	# ad_group_list = sheet.col_values(6)
	impr_list = sheet.col_values(6)
	view_list = sheet.col_values(7)
	cost_list = sheet.col_values(10)
	click_list = sheet.col_values(11)
	print("Finish reading from google sheet!")

	valid_list = [i for i in range(len(channel_url_list)) if "http://youtube.com/channel/" in channel_url_list[i]]
	# print(valid_list)
	# video_url_list = [url_list[i] for i in range(len(url_list)) if "http://youtube.com/video" in url_list[i]]
	channel_url_list = [channel_url_list[i] for i in valid_list]
	# campaign_list = [campaign_list[i] for i in invalid_list]
	# ad_group_list = [ad_group_list[i] for i in invalid_list]
	impr_list = [impr_list[i] for i in valid_list]
	view_list = [view_list[i] for i in valid_list]
	cost_list = [cost_list[i] for i in valid_list]
	click_list = [click_list[i] for i in valid_list]
	print("Delete invalid urls!")

	channel_id_list = [channel.replace("http://youtube.com/channel/", "") for channel in channel_url_list]
	contents = np.transpose([channel_id_list, channel_url_list, impr_list, view_list, click_list, cost_list])
	# print(contents)

	campaign_table = pd.DataFrame(contents, columns=['ID', 'Url', 'Impr', 'View', 'Click','Cost'])
	# print(campaign_table)
	channel_id_list = list(set(channel_id_list))

	# file.write(video_url_list)
	return channel_id_list, campaign_table


def get_data_from_youtube(video_ids, youtube_table):

	os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "0"
	api_service_name = "youtube"
	api_version = "v3"
	client_secrets_file = "YOUR_CLIENT_SECRET_FILE.json"

	# Get credentials and create an API client
	key = "AIzaSyD6DvicKDZODwdXubQxn03Dik8Jja-75NU"
	youtube = googleapiclient.discovery.build(
	api_service_name, api_version, developerKey=key)

	# file = open(result_file, "w+")
	# header = "Video;Channel;Published Date\n"
	# file.write(header)
	# youtube_table = pd.DataFrame(columns=['Video', 'Channel', 'Published Date'])

	itr = math.ceil(len(video_ids) / 50)
	print("Getting data from Youtube API...")

	for i in tqdm(range(0, itr)):

		ids = video_ids[i * 50: i * 50 + 50] if len(video_ids) > i * 50 + 50 else video_ids[i * 50:]
		seperator = ","
		ids_str = seperator.join(ids)
		print(ids_str)

		request = youtube.channels().list(
			part="contentDetails,snippet,statistics,topicDetails,brandingSettings",
			id=ids_str
		)

		response = request.execute()
		# print(response)
		items = response['items']

		for item in items:

			#print(item)
			subs = 0

			v_id = item['id']
			snippet = item['snippet']
			channelTitle = snippet['title']
			branding = item['brandingSettings']

			relevantTopics = []
			topicCategories = []
			country = ''
			bradning_keywords = ''

			if 'country' in snippet:
				country = snippet['country']

			if 'channel' in branding:
				channel_brand = branding['channel']

				if 'keywords' in channel_brand:
					bradning_keywords = channel_brand['keywords']

			if 'topicDetails' in item:
				topic = item['topicDetails']

				if 'topicIds' in topic:
					relevantTopicIds = topic['topicIds']
					#print(relevantTopicIds)
					for t_id in relevantTopicIds:
						if t_id in topic_index:
							relevantTopics.append(topic_index[t_id])

				if 'topicCategories' in topic:
					topicCategories = topic['topicCategories']
					topicCategories = [tpc.replace('https://en.wikipedia.org/wiki/', '') for tpc in topicCategories]

			relevantTopics = list(set(relevantTopics))
			topicCategories = list(set(topicCategories))
			video_info = {'ID': v_id, 'Channel': channelTitle, 'TopicCat': topicCategories, 'Topics': relevantTopics,
						  'Country': country, 'Keywords': bradning_keywords}
			# print(video_info)
			youtube_table = youtube_table.append(video_info, ignore_index=True)

	return youtube_table


def write_to_google_sheet(merged_table, write_sheet):

	sh = client.open(write_worksheet)
	sheet = sh.worksheet(write_sheet)
	sheet.clear()
	gd.set_with_dataframe(sheet, merged_table)
	print(merged_table)


def main():

	channel_id_list, campaign_table = read_videos_from_sheet()
	# keywords_list, campaign_list, ad_group_list = read_keywords_from_sheet()

	youtube_table = pd.DataFrame(columns=['Channel', 'ID', 'TopicCat', 'Topics', 'Country', 'Keywords'])

	finished = False
	url_len = len(channel_id_list)
	print(str(url_len) + " urls!")

	youtube_table = get_data_from_youtube(channel_id_list, youtube_table)
	print("youtube table lens: " + str(len(youtube_table.index)))
	print("url len: " + str(url_len))
	#finished = True if url_len <= len(youtube_table.index) else False
	

	merged_table = campaign_table.merge(youtube_table, on='ID', how='left')
	#merged_table = merged_table.astype({'Impr': 'int32', 'Subscriber': 'int32', 'View': 'int64', 'VideoCount': 'int32'})

	write_to_google_sheet(merged_table, write_sheet1)

	# aggregation_functions = {'ID': 'first', 'Channel': 'first', 'Impr': 'sum', 'TopicCat': 'first', 'Topics': 'first',
	# 						 'Country': 'first', 'Subscriber': 'first', 'View': 'first', 'VideoCount': 'first'}
	# pivoted_table = merged_table.groupby(merged_table['ID']).aggregate(aggregation_functions)

	# write_to_google_sheet(pivoted_table, write_sheet1)

	# lowq_table = pivoted_table.loc[((pivoted_table['Impr'] <= 1) & (pivoted_table['Country'].isin(['CN', 'TW', 'HK']))) | ((pivoted_table['Impr'] <= 10) & (~pivoted_table['Country'].isin(['CN', 'TW', 'HK'])))]
	# lowq_table = lowq_table.loc[((lowq_table['Subscriber'] > 0) & (lowq_table['Subscriber'] <= 5000)) | (lowq_table['VideoCount'] < 20) | (lowq_table['View'] < 100000)]

	# write_to_google_sheet(lowq_table, write_sheet2)





if __name__ == "__main__":
	main()