import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

from urllib.request import urlopen
from shapely.geometry import Point
import geopandas
import math

from selenium import webdriver
import chromedriver_autoinstaller
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


def get_water_wells():
	URL = 'https://apps.ohiodnr.gov/water/maptechs/wellogs/app/'

	u = urlopen(URL)
	try:
		html = u.read().decode('utf-8')
	finally:
		u.close()

	temp = dict()

	num_wells = 0

	# Get list county codes to use later
	soup_counties = BeautifulSoup(html, "html.parser").find_all('option')

	# create an Empty DataFrame object
	df = pd.DataFrame(columns = ['WELL_NO', 'COUNTY', 'TOWNSHIP', 'STREET', 'CASING_DIAMETER', 'TOTAL_DEPTH', 'STATIC_WATER_LEVEL'])

	# For each county number get the list of wells
	for county_option in soup_counties:
		county_no = county_option['value']

		# Loop through each link to roads alphabetically (A-Z, then 1-9)
		modes = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','0','1','2','3','4','5','6','7','8','9']
		for mode in modes:
			# Open the page containing the list of roads
			u = urlopen('https://apps.ohiodnr.gov/water/maptechs/wellogs/app/broad.asp?mode=1&broad=' + mode + '&ccode=' + county_no)
			try:
				html = u.read().decode('utf-8')
			finally:
				u.close()
			roads = BeautifulSoup(html, "html.parser").find_all('ul')[0].find_all('a') # unordered list of all roads for this county

			# For each road in this list of a tags, open the page with the list of wells on   
			for road in roads: 
				# Open the page containing the list of wells for this road
				#print('https://apps.ohiodnr.gov/water/maptechs/wellogs/app/' + road['href'])
				u = urlopen('https://apps.ohiodnr.gov/water/maptechs/wellogs/app/' + road['href'])
				try:
					html = u.read().decode('utf-8')
				finally:
					u.close()

				# Find all links to well pages
				wells = BeautifulSoup(html, "html.parser").find_all('ul')[0].find_all('a')
				if len(wells)>0:
					# For each well on the page, concat the pandas dataf frame with the amount of water
					for well in wells:
						#print(well['href'])
						u = urlopen('https://apps.ohiodnr.gov' + well['href'])
						try:
							html = u.read().decode('utf-8')
						finally:
							u.close()

						datasoup = BeautifulSoup(html, "html.parser")

						#print(datasoup.text)
						#input("Wait...")

						# From this page, want well number, county, township, street, cubic feet of water
						well_no = datasoup.find(id="WellLogNo").getText()
						county = datasoup.find(id="County").getText()
						township = datasoup.find(id="Township").getText()
						street = datasoup.find(id="Address").getText()
						casingdiameter = datasoup.find(id="CasingDiameter1").getText()
						totaldepth = datasoup.find(id="TotalDepth").getText()
						staticwaterlevel = datasoup.find(id="StaticWaterLevel").getText()
						lat = datasoup.find(id="Latitude").getText()
						lon = datasoup.find(id="Longitude").getText()
						if len(lat) == 0 or len(casingdiameter) == 0 or len(totaldepth) == 0 or len(staticwaterlevel) == 0:
							continue
						tot_vol = f'{round(math.pi * ((float(casingdiameter[:casingdiameter.index(" ")])/24)**2 * (float(totaldepth[:totaldepth.index(" ")])-float(staticwaterlevel[:staticwaterlevel.index(" ")]))), 2)} cubic ft.'

						# Append the new well data to df
						#temp[well_no] = [county, township, street, casingdiameter, totaldepth, staticwaterlevel]
						newwell = pd.DataFrame([[well_no,county,township,street,casingdiameter,totaldepth,staticwaterlevel, lat, lon, tot_vol]],columns=['WELL_NO', 'COUNTY', 'TOWNSHIP', 'STREET', 'CASING_DIAMETER', 'TOTAL_DEPTH', 'STATIC_WATER_LEVEL', 'LATITUDE', 'LONGITUDE', 'TOTAL_VOLUME'])
						df = df.append(newwell)
						num_wells += 1
						print(num_wells)
						if num_wells > 20:
							df['GEOMETRY'] = df.apply(lambda x: Point((float(x.LONGITUDE), float(x.LATITUDE))), axis=1)
							df = geopandas.GeoDataFrame(df, geometry='GEOMETRY')
							df.to_file('MyGeometries.shp', driver='ESRI Shapefile')
							df = df.drop('GEOMETRY', axis=1)
							print(df)
							input("wasss")
							return df
						#df2 = pd.DataFrame([[key] + value for key, value in temp.items()], columns=['Well_No', 'County', 'Township', 'Street', 'CasingDiameter', 'TotalDepth', 'StaticWaterLevel'])
						#print(df)
						#print(df2)
						#input("check")
	print("Done")


def establish_connection():
    print("Establishing connection to Snowflake...")

    conn = snow.connect(
        user='Jtmaxson',
        password='Adminpw123?',
        account='qq38878.us-east-2.aws'
    )
    cur = conn.cursor()
    return conn, cur

def close_connection(conn, cur):
    print("Closing connection...")

    cur.close()
    conn.close()

def compile_data(dict_list):
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
    return {key: round(value[1] * value[2] / value[0], 2) for key, value in d.items()}

def populate_table(conn, cur, df, table_name, cols):
    print(f'Populating table: {table_name}...')

    cur.execute(f'CREATE OR REPLACE TABLE {table_name}({cols})')
    write_pandas(conn, df, table_name)

def initialize_schema(cur):
    print("Navigating to proper schema...")

    cur.execute("USE ROLE ACCOUNTADMIN")
    cur.execute("""CREATE WAREHOUSE IF NOT EXISTS RADMAP_WH 
             WITH WAREHOUSE_SIZE = XSMALL""")
    cur.execute("USE WAREHOUSE RADMAP_WH")
    cur.execute("CREATE DATABASE IF NOT EXISTS RADMAP_DB")
    cur.execute("USE DATABASE RADMAP_DB")
    cur.execute("CREATE SCHEMA IF NOT EXISTS RADMAP_SCHEMA")
    cur.execute("USE SCHEMA RADMAP_SCHEMA")

def update_data():
    raw_data = compile_data([get_bank_data(), get_population_data(), get_land_mass_data()])
    fdrs = calc_fdrs(raw_data)
    raw_data_df = pd.DataFrame([[key] + value for key, value in raw_data.items()], columns=["COUNTY", 'BANK_OFFICES', 'POPULATION', 'LAND_MASS'])
    fdrs_df = pd.DataFrame([[key, value] for key, value in fdrs.items()], columns=["COUNTY", 'FDRS'])
    conn, cur = establish_connection()
    initialize_schema(cur)
    populate_table(conn, cur, raw_data_df, "BANK_DATA_TABLE", "County string, Bank_Offices integer, Population integer, Land_Mass float")
    populate_table(conn, cur, fdrs_df, "FDRS_TABLE", "County string, FDRS float")
    close_connection(conn, cur)

def get_bank_data(year="2021"):
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
    print("Fetching population data...")

    pop_data = requests.get(f'https://api.census.gov/data/2019/pep/charagegroups?get=NAME,POP&HISP=0&for=county:*&in=state:39')
    json_data = json.loads(pop_data.text)[1:] # ignore the header row
    return {row[0][:(row[0].index("County")-1)] : int(row[1]) for row in json_data}

def get_land_mass_data():
    print("Fetching land mass data...")

    land_page = requests.get("https://www.indexmundi.com/facts/united-states/quick-facts/ohio/land-area#table")
    soup = BeautifulSoup(land_page.content, "html.parser")
    return {row.find("td").text : float(row.find("td", align="right").text) for row in soup.find("tbody").findChildren("tr")}

def update_water_wells():
	water_df = get_water_wells()
	conn, cur = establish_connection()
	initialize_schema(cur)
	populate_table(conn, cur, water_df, "WATER_WELL_TABLE", "Well_No string, County string, Township string, Street string, Casing_Diameter string, Total_Depth string, Static_Water_Level string, Latitude string, Longitude string, Total_Volume string")
	close_connection(conn, cur)

def init_chrome_driver(url):
	chromedriver_autoinstaller.install()
	options = webdriver.ChromeOptions()
	options.add_argument('--window-size=1920,1080')
	options.add_argument('--no-sandbox')
	options.add_argument('--headless')
	options.add_argument('--disable-dev-sh-usage')
	driver = webdriver.Chrome(chrome_options=options)
	driver.maximize_window()
	driver.implicitly_wait(20)
	driver.get(url)
	return driver

def click_dropdown_element(driver, xpath):
	s = driver.find_element_by_xpath(xpath)
	actions = ActionChains(driver)
	actions.move_to_element(s).perform()
	wait(driver, 5).until(EC.visibility_of_element_located((By.XPATH, xpath))).click()
	driver.implicitly_wait(50)

def get_covid_data(ohio_only=False):
	print(f'Getting covid data for {"Ohio" if ohio_only else "USA"}...')
	# open a simulated chrome page with the given url
	driver = init_chrome_driver("https://covid.cdc.gov/covid-data-tracker/#trends_dailycases")

	# click on the dropdown menu
	driver.find_element_by_xpath("//div[@class='ui search selection dropdown']").click()

	if(ohio_only):
		# click on Ohio from the dropdown menu
		click_dropdown_element(driver, "//div[@data-value='39']")

	# fetch and store daily covid cases
	innerHTML = driver.execute_script("return document.body.innerHTML")
	soup = BeautifulSoup(innerHTML, 'html.parser')
	driver.close()
	return {row.find_all("td")[1].text : int(row.find_all("td")[2].text.replace(",", "")) for row in soup.find("tbody").findChildren("tr")}

def convert_to_timeseries(x):
	month_to_num = {"Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06", "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}
	times = [element for element in x.split(" ") if len(element) > 0]
	return f'{times[2]}-{month_to_num[times[0]]}-{times[1].replace(",", "").zfill(2)}'

def update_covid_data():
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
