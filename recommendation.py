import requests, json, os, sys, time, re
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import *
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel,cosine_similarity
from sklearn.cluster import KMeans
import numpy as np
from pyspark.mllib.recommendation import ALS
from pyspark import SparkContext

def show_work_status(singleCount, totalCount, currentCount=0):
	currentCount += singleCount
	percentage = 100.0 * currentCount / totalCount
	status =  '>' * int(percentage)  + '=' * (100 - int(percentage))
	sys.stdout.write('\r[{0}] {1:.2f}% '.format(status, percentage))
	sys.stdout.flush()
	if percentage >= 100:
		print ('\n')



# set file path
path_app_info = 'data/app_detail.txt'
path_app_stats = 'data/2017-04-14.json'
path_user_inventory = 'data/user_inventory_sample.txt'

engine = create_engine('mysql+pymysql://app_dba:2020-Develop@127.0.0.1/game_recommendation?charset=utf8mb4') # local mysql server
engine.execute('ALTER DATABASE game_recommendation CHARACTER SET = utf8mb4')

#####################################
### extract selected app features ###
#####################################


with open(path_app_stats,'rb') as f:
	dic_steamspy = json.load(f)


with open(path_app_info, 'rb') as f:
	dic_steam_app = {'initial_price':{},'name':{},'score':{},'windows':{},'mac':{},'linux':{},'type':{},'release_date':{},'recommendation':{},'header_image':{},'currency':{},'success':{}}
	dic_about_the_game = {}
	lst_raw_string = f.readlines()
	total_count = len(lst_raw_string)
	current_count = 0
	for raw_string in lst_raw_string:
		steam_id,app_data = list(json.loads(raw_string).items())[0]
		if app_data == {}:
			dic_steam_app['success'].update({steam_id:False})
		else:
			initial_price = app_data.get('price_overview',{}).get('initial')
			currency = app_data.get('price_overview',{}).get('currency')
			if app_data.get('is_free') == True:
				initial_price = 0
			app_name = app_data.get('name')
			critic_score = app_data.get('metacritic', {}).get('score')
			app_type = app_data.get('type')
			for (platform, is_supported) in app_data.get('platforms').items():
				if is_supported == True:
					dic_steam_app[platform].update({steam_id:1})	
			if app_data.get('release_date',{}).get('coming_soon') == False:
				about_the_game = app_data.get('about_the_game')
				soup = BeautifulSoup(about_the_game,'lxml')
				game_description = re.sub(r'(\s+)',' ',soup.text).strip()
				dic_about_the_game.update({steam_id:game_description})
				release_date = app_data.get('release_date',{}).get('date')
				if not release_date == '':
					if re.search(',', release_date) == None:
						release_date = datetime.strptime(release_date, '%b %Y')
					else:
						try:
							release_date = datetime.strptime(release_date, '%b %d, %Y')
						except:
							release_date = datetime.strptime(release_date, '%d %b, %Y')
			recommendation = app_data.get('recommendations',{}).get('total')
			header_image = app_data.get('header_image')
			dic_steam_app['initial_price'].update({steam_id:initial_price})
			dic_steam_app['currency'].update({steam_id:currency})
			dic_steam_app['name'].update({steam_id:app_name})
			dic_steam_app['score'].update({steam_id:critic_score})
			dic_steam_app['type'].update({steam_id:app_type})
			dic_steam_app['release_date'].update({steam_id:release_date})
			dic_steam_app['recommendation'].update({steam_id:recommendation})
			dic_steam_app['header_image'].update({steam_id:header_image})
		show_work_status(1, total_count, current_count)
		current_count += 1


df_steam_app = pd.DataFrame(dic_steam_app)
df_steam_app.initial_price = df_steam_app.initial_price.map(lambda x: x/100.0)
df_steam_app.index.name = 'steam_appid'
df_steam_app['windows'] = df_steam_app.windows.fillna(0)
df_steam_app['mac'] = df_steam_app.mac.fillna(0)
df_steam_app['linux'] = df_steam_app.linux.fillna(0)
df_steam_app = df_steam_app[['name', 'type', 'currency', 'initial_price', 'release_date', 'score', 'recommendation', 'windows', 'mac', 'linux', 'success','header_image']]
df_steam_app.reset_index(inplace=True)
df_steam_app.success.fillna(True, inplace=True)
df_steam_app.to_sql('tbl_app_info',engine,if_exists='replace',index=False)


#####################################
#### Most Played Games Per User  ####
#####################################
dic_user_favorite_app = {}
with open(path_user_inventory, 'r') as f:
	for raw_string in f.readlines():
		user_id, lst_inventory = list(json.loads(raw_string).items())[0]
		if lst_inventory != None and lst_inventory != []:
			most_played_app_id = sorted(lst_inventory, key=lambda k: k['playtime_forever'])[-1].get('appid')
		else:
			most_played_app_id = None
		dic_user_favorite_app.update({user_id:most_played_app_id})
df_user_favorite_app = pd.Series(dic_user_favorite_app).to_frame().reset_index()
df_user_favorite_app.columns = ['steam_user_id','most_favorite_app']
df_user_favorite_app.to_sql('tbl_user_favorite_app', engine, if_exists='replace', index=False)


#####################################
#### Build Recommendation Models ####
#####################################


df_steam_app = pd.read_sql('tbl_app_info',engine)
df_valid_games = df_steam_app.query('success == True and type == "game" and release_date <= "{}" and initial_price >= 0'.format(datetime.today().date().isoformat()))
set_valid_game_id = set(df_valid_games.steam_appid)


# Model 1: Popularity Based
print ('Popularity Based Model')
df_popularity_based_results = pd.Series(list(dic_steamspy.get('owners').values()),list(dic_steamspy.get('owners').keys())).sort_values(ascending=False).to_frame()
df_popularity_based_results.index.name = 'steam_appid'
df_popularity_based_results.reset_index(inplace=True)
df_popularity_based_results.to_sql('tbl_results_popularity_based',engine,if_exists='replace')


# Model 2: Content based - Description
print ('Content Based Model')

for i in list(set(dic_about_the_game.keys()) - set(df_valid_games.steam_appid)):
	del dic_about_the_game[i]


tfidf = TfidfVectorizer(strip_accents='unicode',stop_words='english').fit_transform(dic_about_the_game.values())
lst_app_id = list(dic_about_the_game.keys())
dic_recomended = {}
total_count = len(lst_app_id)
current_count = 0
for index in range(tfidf.shape[0]):
	cosine_similarities = linear_kernel(tfidf[index:index+1], tfidf).flatten()
	related_docs_indices = cosine_similarities.argsort()[-2:-22:-1]
	dic_recomended.update({lst_app_id[index]:[lst_app_id[i] for i in related_docs_indices]})
	show_work_status(1,total_count,current_count)
	current_count+=1


df_content_based_results = pd.DataFrame(dic_recomended).T
df_content_based_results.index.name = 'steam_appid'
df_content_based_results.reset_index(inplace=True)
df_content_based_results.to_sql('tbl_results_content_based',engine,if_exists='replace')


# Model 3: item based
print ('Item Based Model')

dic_purchase = {}
with open(path_user_inventory,'rb') as f:
	lst_all = f.readlines()
	total_count = len(lst_all)
	current_count = 0
	for i in lst_all:
		user_id, user_inventory = list(json.loads(i).items())[0]
		if user_inventory != [] and user_inventory != None and user_inventory != {}:
			dic_purchase[user_id] = {}
			for playtime_info in user_inventory:
				appid = playtime_info.get('appid')
				if str(appid) in set_valid_game_id:
					dic_purchase[user_id].update({appid:1})
		show_work_status(1,total_count,current_count)
		current_count+=1


df_purchase = pd.DataFrame(dic_purchase).fillna(0)
purchase_matrix = df_purchase.values
lst_user_id = df_purchase.columns
lst_app_id = df_purchase.index

total_count = purchase_matrix.shape[0]
current_count = 0

dic_recomended_item_based = {}
for index in range(total_count):
	cosine_similarities = linear_kernel(purchase_matrix[index:index+1], purchase_matrix).flatten()
	lst_related_app = np.argsort(-cosine_similarities)[1:101]
	dic_recomended_item_based.update({lst_app_id[index]:[lst_app_id[i] for i in lst_related_app]})
	show_work_status(1,total_count,current_count)
	current_count+=1


df_item_based_result = pd.DataFrame(dic_recomended_item_based).T
df_item_based_result.index.name = 'steam_appid'
df_item_based_result.reset_index(inplace=True)
df_item_based_result.to_sql('tbl_results_item_based',engine,if_exists='replace')



# Model 4: Collaborative Filtering
# NOTE: This model requires PySpark

print ('ALS Model')
sc = SparkContext()

def parse_raw_string(raw_string):
	user_inventory = json.loads(raw_string)
	return list(user_inventory.items())[0]


def id_index(x):
	((user_id,lst_inventory),index) = x
	return (index, user_id)


def create_tuple(x):
	((user_id,lst_inventory),index) = x
	if lst_inventory != None:
		return (index, [(i.get('appid'), 1) for i in lst_inventory if str(i.get('appid')) in set_valid_game_id])
	else:
		return (index, [])

def reshape(x):
	(index,(appid,time)) = x
	return (index,appid,time)

user_inventory_rdd = sc.textFile(path_user_inventory).map(parse_raw_string).zipWithIndex()
dic_id_index = user_inventory_rdd.map(id_index).collectAsMap()
#training_rdd = user_inventory_rdd.map(create_tuple).flatMapValues(lambda x: x).map(lambda (index,(appid,time)):(index,appid,time))
training_rdd = user_inventory_rdd.map(create_tuple).flatMapValues(lambda x: x).map(reshape)

model = ALS.train(training_rdd, 5)

dic_recommended = {}
for index in dic_id_index.keys():
	try:
		lst_recommended = [i.product for i in model.recommendProducts(index,50)]
		user_id = dic_id_index.get(index)
		dic_recommended.update({user_id:lst_recommended})
	except:
		pass


df_als_result = pd.DataFrame(dic_recommended).T
df_als_result.index.name = 'steam_user_id'
df_als_result.reset_index(inplace=True)
df_als_result.to_sql('tbl_results_als_based',engine,if_exists='replace')

print ('Finished')




