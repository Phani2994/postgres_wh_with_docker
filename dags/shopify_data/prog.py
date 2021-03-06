import requests
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import argparse
from datetime import date, timedelta
import sys



list_of_issues=[]

def create_table():
	create_table_sql = """CREATE TABLE IF NOT EXISTS shopify_data
    (
	id text NULL,
	shop_domain text NULL,
	application_id text NULL,
	autocomplete_enabled bool NULL,
	user_created_at_least_one_qr bool NULL,
	nbr_merchandised_queries int8 NULL,
	nbrs_pinned_items text NULL,
	showing_logo bool NULL,
	has_changed_sort_orders bool NULL,
	analytics_enabled bool NULL,
	use_metafields bool NULL,
	nbr_metafields float8 NULL,
	use_default_colors bool NULL,
	show_products bool NULL,
	instant_search_enabled bool NULL,
	instant_search_enabled_on_collection bool NULL,
	only_using_faceting_on_collection bool NULL,
	use_merchandising_for_collection bool NULL,
	index_prefix text NULL,
	indexing_paused bool NULL,
	install_channel text NULL,
	export_date text NULL,
	has_specific_prefix text NULL
    )
	"""

	conn = psycopg2.connect("host=warehouse dbname=algolia_wh user=algolia_user password=algolia_pwd")
	cur = conn.cursor()
	cur.execute("{0}".format(create_table_sql))
	cur.close()
	conn.commit()
		

def process_data(user_id, pw, db, date):
	try:
		URL = "https://alg-data-public.s3.amazonaws.com/{0}.csv".format(date)

		# download the data behind the URL
		response = requests.get(URL)
		# Open the response into a new file
		open("2019-04-02.csv", "wb").write(response.content)

		df1 = pd.read_csv('2019-04-02.csv') 
		df1= df1[df1.application_id.notnull()]
		df1.loc[df1['index_prefix'] != "shopify_", 'has_specific_prefix'] = True
		df1.loc[df1['index_prefix'] == "shopify_", 'has_specific_prefix'] = False
		# I would prefer to take a back up of the processed data as a csv for each extract, in case if we need to re enter the data in database or for any checks.
		# df1.to_csv('{0}-filtered.csv'.format(date), index=False)
		engine = create_engine('postgresql://{0}:{1}@warehouse:5432/{2}'.format(user_id, pw, db))
		df1.to_sql('shopify_data', engine, index=False, if_exists='append')
		print("data processed for the date - {0}".format(date))
		print("row count - {0}".format(df1.shape[0]))
		return df1.shape[0]
	except Exception as e:
		print("Unable to process data for date - {0}".format(date))
		print(e)
		list_of_issues.append("Unable to process data for date - {0}. {1}".format(date, e))
		return int(0)

def data_check_before_import(date):  
	print("data check before import for the date - {0} started".format(date))
	try:
		conn = psycopg2.connect("host=warehouse dbname=algolia_wh user=algolia_user password=algolia_pwd")
	except:
		print("Unable to connect to the database.")
	else:
		cur = conn.cursor()
		# When we schedule this job for a daily run, we would need to make sure that the data is not already entered, to avoid duplicates.
		# we could use {{ ds }} with code logic  to make sure that data is up to date till the current date and also avoid duplicates.
		# cur.execute('select * FROM shopify_data where export_date={0}'.format('{{ ds }}'))
		cur.execute("select count(*) FROM shopify_data where export_date='{0}'".format(date))
		rows = cur.fetchall()
		print(rows)
		try:
			if rows[0][0]>0:
				raise Exception("Records already exist!!")
		except Exception as error:
			print(error)
			return False
		return True
	print("data check before import for the date - {0} completed".format(date))

def data_check_after_import(date, rowcount):  
	print("data check after import for the date - {0} started".format(date))
	try:
		conn = psycopg2.connect("host=warehouse dbname=algolia_wh user=algolia_user password=algolia_pwd")
	except:
		print("Unable to connect to the database.")
	else:
		cur = conn.cursor()
		# When we schedule this job for a daily run, we would need to make sure that the data is not already entered, to avoid duplicates.
		# we could use {{ ds }} to make sure that data is up to date till the current date and also avoid duplicates.
		# cur.execute('select * FROM shopify_data where export_date={0}'.format('{{ ds }}'))
		cur.execute("select count(*) FROM shopify_data where export_date='{0}'".format(date))
		rows = cur.fetchall()
		print(rows)
		print(rows[0][0])
		print(type(rows[0][0]))
		try:
			if rows[0][0]!=rowcount:
				raise Exception("Records count for the date- {0} does not match. Check if all the records are inserted.".format(date))
		except Exception as error:
			print(error)
			list_of_issues.append(error)
	print("data check after import for the date - {0} completed".format(date))

def main(user_id, pw, db):
	create_table()
	d2 = date(2019,4,1) # In this example we are passing ifnormation static as per the scope of the aim. But we could pass date as a parameter, by running airflow dag with configs.
	while d2 < date(2019,4,8):
		d1 = d2.isoformat()
		print(d1)
		d2=d2+timedelta(days=1)
		flag = data_check_before_import(d1)
		print(flag)
		if flag:
			row_count= process_data(user_id, pw, db, d1)
			data_check_after_import(d1, row_count)
		else:
			print("Records already exist for the date - {0}. Skipping for this date. Check if you are inserting duplicates".format(d1))
			list_of_issues.append("Records already exist for the date - {0}. Skipping for this date. Check if you are inserting duplicates".format(d1))
	
	if list_of_issues:
		print("ERROR!! - We have some issues with the data processing in this job run. Check the list  of errors - {0}".format(list_of_issues))
		sys.exit()
	


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="nothing")
    parser.add_argument(
        "--user_id",
        required=True,
        help="postgres db user_id",
    )
    parser.add_argument(
        "--pw",
        required=True,
        help="postgres db password",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="postgres db name",
    )
    args = parser.parse_args()
    main(user_id=args.user_id,pw=args.pw, db=args.db, )