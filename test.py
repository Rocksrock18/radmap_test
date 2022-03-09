import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

import math
import time

from selenium import webdriver
import chromedriver_autoinstaller
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


def establish_connection():
	"""Establishes a connection to a Snowflake instance

	Returns
	_______
	snowflake connection
		The current Snowflake connection
	snowflake cursor
		The current Snowflake cursor

	"""
	print("Establishing connection to Snowflake...")
	conn = snow.connect(
		user='Jtmaxson',
		password='Adminpw123?',
		account='qq38878.us-east-2.aws'
	)
	cur = conn.cursor()
	return conn, cur

def close_connection(conn, cur):
	"""Closes a connection to a Snowflake instance

	Parameters
	__________
	conn
		The current Snowflake connection
	cur
		The current Snowflake cursor

	"""
	print("Closing connection...")
	cur.close()
	conn.close()

def merge_data(dict_list):
	"""Merges multiple dictionaries into a single dictionary

	Parameters
	__________
	dict_list : list(dict)
		List of dictionaries to be merged

	Returns
	_______
	dict
		The merged dictionary

	"""
	print("Compiling data...")

	data = dict()
	for d in dict_list:
		for key, value in d.items():
			if key in data:
				data[key].append(value)
			else:
				data[key] = [value]
	return data

def calc_fdrs(d):
	"""Calculates Financial Desert Risk Score based on the land mass, population, and number of banks in a given county for all counties

	Parameters
	__________
	d : dict
		Dictionary containing land mass, population, and bank data for all counties

	Returns
	_______
	dict
		The dictionary containing FDRS for all counties

	"""
	return {key: round(value[1] * value[2] / value[0], 2) for key, value in d.items()}

def populate_table(conn, cur, df, table_name, cols):
	"""Populates a table in Snowflake with the given data

	Parameters
	__________
	conn : snowflake connection
		The current Snowflake connection
	cur : snowflake cursor
		The current Snowflake cursor
	df : DataFrame
		Dataframe containing the data to be imported into Snowflake
	table_name : str
		Name of the table to be populated
	cols : str
		String of column names and data types used by Snowflake to populate table

	"""
	print(f'Populating table: {table_name}...')

	cur.execute(f'CREATE OR REPLACE TABLE {table_name}({cols})')
	write_pandas(conn, df, table_name)

def initialize_schema(cur):
	"""Navigates to the correct schema in Snowflake

	Parameters
	__________
	cur : snowflake cursor
		Snowflake cursor

	"""
	print("Navigating to proper schema...")

	cur.execute("USE ROLE ACCOUNTADMIN")
	cur.execute("""CREATE WAREHOUSE IF NOT EXISTS RADMAP_WH 
             WITH WAREHOUSE_SIZE = XSMALL""")
	cur.execute("USE WAREHOUSE RADMAP_WH")
	cur.execute("CREATE DATABASE IF NOT EXISTS RADMAP_DB")
	cur.execute("USE DATABASE RADMAP_DB")
	cur.execute("CREATE SCHEMA IF NOT EXISTS RADMAP_SCHEMA")
	cur.execute("USE SCHEMA RADMAP_SCHEMA")

def update_financial_data():
	"""Updates all financial data in Snowflake

	"""
	raw_data = merge_data([get_bank_data(), get_population_data(), get_land_mass_data()])
	fdrs = calc_fdrs(raw_data)
	raw_data_df = pd.DataFrame([[key] + value for key, value in raw_data.items()], columns=["COUNTY", 'BANK_OFFICES', 'POPULATION', 'LAND_MASS'])
	fdrs_df = pd.DataFrame([[key, value] for key, value in fdrs.items()], columns=["COUNTY", 'FDRS'])
	conn, cur = establish_connection()
	initialize_schema(cur)
	populate_table(conn, cur, raw_data_df, "BANK_DATA_TABLE", "County string, Bank_Offices integer, Population integer, Land_Mass float")
	populate_table(conn, cur, fdrs_df, "FDRS_TABLE", "County string, FDRS float")
	close_connection(conn, cur)

def get_bank_data(year="2021"):
	"""Fetches the number of FDIC approved bank offices in each county

	Parameters
	__________
	year : str
		The year of bank data to fetch. Default = 2021

	Returns
	_______
	dict
		The dictionary containing the number of FDIC approved bank offices in each county

	"""
	print("Fetching bank data...")

    # We need to navigate from the base page to the desired page to ensure cookies are correct, otherwise we get error code 500
	session = requests.session()
	session.get("https://www7.fdic.gov/sod/sodSummary.asp?barItem=3")
	session.post("https://www7.fdic.gov/sod/SODSummary2.asp", data={"InfoAsOf": year, "barItem": "3", "sSummaryList": "8"})
	bank_page = session.post("https://www7.fdic.gov/sod/SODSumReport.asp", data={"sState": "Ohio", "InfoAsOf": year, "submit1": "Continue", "barItem": "3"})

    # Parse page to return dict of form {county_name : num_bank_offices}
	soup = BeautifulSoup(bank_page.content, "html.parser")
	rows = [county.parent for county in soup.find_all("td", headers="hdr_county")]
	return {row.find("td", headers="hdr_county").text : int(row.find("td", headers="hdr_all_offices").text) for row in rows}

def get_population_data():
	"""Fetches the population of each county

	Returns
	_______
	dict
		The dictionary containing the population of each county

	"""
	print("Fetching population data...")

	# send request to census api
	pop_data = requests.get(f'https://api.census.gov/data/2019/pep/charagegroups?get=NAME,POP&HISP=0&for=county:*&in=state:39')
	json_data = json.loads(pop_data.text)[1:]
	return {row[0][:(row[0].index("County")-1)] : int(row[1]) for row in json_data}

def get_land_mass_data():
	"""Fetches the land mass of each county

	Returns
	_______
	dict
		The dictionary containing the land mass of each county

	"""
	print("Fetching land mass data...")

	land_page = requests.get("https://www.indexmundi.com/facts/united-states/quick-facts/ohio/land-area#table")
	soup = BeautifulSoup(land_page.content, "html.parser")
	return {row.find("td").text : float(row.find("td", align="right").text) for row in soup.find("tbody").findChildren("tr")}

def init_chrome_driver(url):
	"""Initializes a chrome driver open on a given url

	Parameters
	__________
	url : str
		The url to navigate to

	Returns
	_______
	selenium Chrome
		The chrome driver which simulates a chrome environment

	"""
	# auto install the chrome web driver
	chromedriver_autoinstaller.install()

	# specify chrome browser options
	opt = webdriver.ChromeOptions()
	opt.add_argument('--window-size=1920,1080')
	opt.add_argument('--no-sandbox')
	opt.add_argument('--headless')
	opt.add_argument('--disable-dev-sh-usage')
	driver = webdriver.Chrome(options=opt)

	# maximize window to ensure clicks occur in correct locations
	driver.maximize_window()
	driver.get(url)

	# sometimes covid table takes time to load, so sleep
	time.sleep(10)
	return driver

def click_dropdown_element(driver, xpath):
	"""Clicks on a specific element from a dropdown menu

	Parameters
	__________
	driver : selenium Chrome
		The chrome web driver
	xpath : str
		The xpath of the element to click on

	"""
	s = driver.find_element(by=By.XPATH, value=xpath)
	actions = ActionChains(driver)

	# scrolls through dropdown menu until it finds the specified element, then clicks on it
	actions.move_to_element(s).perform()
	wait(driver, 5).until(EC.visibility_of_element_located((By.XPATH, xpath))).click()

def get_covid_data(ohio_only=False):
	"""Fetches the number of new COVID cases each day for Ohio or for USA as a whole

	Parameters
	__________
	ohio_only : bool
		Boolean dictating whether the data fetched should be from Ohio only, or the USA as a whole

	Returns
	_______
	dict
		The dictionary containing the number of new COVID cases each day for Ohio or for USA as a whole

	"""
	print(f'Getting covid data for {"Ohio" if ohio_only else "USA"}...')

	driver = init_chrome_driver("https://covid.cdc.gov/covid-data-tracker/#trends_dailycases")

	# if we want Ohio, we need to click on Ohio from the dropdown menu
	if(ohio_only):
		driver.find_element(by=By.XPATH, value="//div[@class='ui search selection dropdown']").click()
		click_dropdown_element(driver, "//div[@data-value='39']")
		time.sleep(5)

	innerHTML = driver.execute_script("return document.body.innerHTML")
	soup = BeautifulSoup(innerHTML, 'html.parser')
	driver.close()
	return {row.find_all("td")[1].text : int(row.find_all("td")[2].text.replace(",", "")) for row in soup.find("tbody").findChildren("tr")}

def convert_to_timeseries(x):
	"""Converts to a string of format "Month Day, Year" to format "YYYY-MM-DD

	Parameters
	__________
	x : str
		String in "Month Day, Year" format

	Returns
	_______
	str
		String in "YYYY-MM-DD" format

	"""
	month_to_num = {"Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06", "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}
	times = [element for element in x.split(" ") if len(element) > 0]
	return f'{times[2]}-{month_to_num[times[0]]}-{times[1].replace(",", "").zfill(2)}'

def update_covid_data():
	"""Updates all COVID data in Snowflake

	"""
	covid_ohio = get_covid_data(True)
	covid_usa = get_covid_data()

	# convert dicts to dataframes
	covid_ohio_df = pd.DataFrame([[key, value] for key, value in covid_ohio.items()], columns=["DATE", 'NEW_CASES'])
	covid_usa_df = pd.DataFrame([[key, value] for key, value in covid_usa.items()], columns=["DATE", 'NEW_CASES'])
	covid_ohio_df['DATE'] = covid_ohio_df['DATE'].apply(convert_to_timeseries)
	covid_usa_df['DATE'] = covid_usa_df['DATE'].apply(convert_to_timeseries)

	# send data to snowflake
	conn, cur = establish_connection()
	initialize_schema(cur)
	populate_table(conn, cur, covid_ohio_df, "OHIO_COVID_TABLE", "Date string, New_Cases integer")
	populate_table(conn, cur, covid_usa_df, "USA_COVID_TABLE", "Date string, New_Cases integer")
	close_connection(conn, cur)

update_covid_data()
update_financial_data()
print("Finished!")
