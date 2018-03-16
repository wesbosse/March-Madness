import requests
from bs4 import BeautifulSoup
import pymysql

def scrape_kenpom():
	data_to_sql = []
	columns_to_sql_with_type = ['year int', 'kp_rank int','team varchar(32)','conference varchar(10)','win_loss varchar(10)','efficiency_margin float','offensive_efficiency float','rank_oe int','defensive_efficiency float','rank_de int','tempo float','rank_tempo int','luck float','rank_luck int','strength_of_schedule float','rank_sos int','avg_opponent_oe float','rank_ooe int','avg_opponent_de float','rank_ode int','non_conference_sos float','rank_ncsos int']
	columns_to_sql = ['year', 'kp_rank','team','conference','win_loss','efficiency_margin','offensive_efficiency','rank_oe','defensive_efficiency','rank_de','tempo','rank_tempo','luck','rank_luck','strength_of_schedule','rank_sos','avg_opponent_oe','rank_ooe','avg_opponent_de','rank_ode','non_conference_sos','rank_ncsos']
	base_url = 'https://kenpom.com/'
	response = requests.get(base_url)
	links = BeautifulSoup(response.content, 'lxml').find('div', {'id': 'content-header'}).find('span', {'class': 'rank'}).findAll('a')

	url_list = []
	for link in links:
		if link.text[0] == '2':
			url_list.append(base_url + link.attrs.get('href'))
	url_list.append(base_url + 'index.php')

	for url in url_list:
		response = requests.get(url)
		soup = BeautifulSoup(response.content, 'lxml')
		year = soup.find('div', {'id': 'content-header'}).find('h2').text[:4]
		data_tables = soup.find('table', {'id': 'ratings-table'}).findAll('tbody')

		for table in data_tables:
			teams = table.findAll('tr')

			for team in teams:
				stats = team.findAll('td')
				team_data = [year]

				for stat in stats:
					stat_links = stat.findAll('a')
					if len(stat_links):
						stat = stat_links[0]
					stat_text = stat.text.replace('+', '')
					team_data.append(stat_text)

				if len(team_data) > 1:
					data_to_sql.append(team_data)
		
		print('Scrape complete for ' + year)
	return data_to_sql, columns_to_sql, columns_to_sql_with_type

def push_to_db(data, columns, columns_with_type):
	host = '198.71.225.53'
	user = input("Username: ")
	password = input("Password: ")
	db = 'ncaa'
	table_name = "Scraped"
	
	connection = pymysql.connect(
	    host=host,
	    user=user,
	    password=password,
	    db=db
	)
	cursor = connection.cursor()

	cursor.execute("SHOW TABLES LIKE '{}'".format(table_name))
	result = cursor.fetchone()
	print(result)
	if result == None:
		cursor.execute("CREATE TABLE {} ({})".format(table_name, ','.join(columns_with_type)))
		connection.commit()
	else:
		cursor.execute("DROP TABLE {}".format(table_name))
		cursor.execute("CREATE TABLE {} ({})".format(table_name, ','.join(columns_with_type)))
		connection.commit()

	for row in data:
		formatted_row=[]
		for item in row:
			if type(item) == str:
				formatted_row.append('"' + item + '"')
			else:
				formatted_row.append(item)


		query = "INSERT INTO {} ({}) VALUES ({})".format(table_name, ','.join(columns), ','.join(formatted_row))
		print(query)
		cursor.execute(query)

	connection.commit()

data, columns, columns_with_type = scrape_kenpom()
push_to_db(data, columns, columns_with_type)