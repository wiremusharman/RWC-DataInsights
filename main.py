from bs4 import BeautifulSoup
import requests
import pyodbc

rankings = []
schedule = []

server = 'LAPTOP-0KIQI337,1433'
db = 'RAW_RWC'

try:
    # print(pyodbc.drivers())
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          'SERVER=' + server + ';'
                          'DATABASE=' + db + ';'
                          'Trusted_Connection=yes;')
except pyodbc.Error as ex:
    print('Error connecting to the database: ', ex)
cursor = cnxn.cursor()


def get_rankings():

    # Scraping function to get world rugby rankings
    rankings_base_url = 'https://www.the-sports.org/rugby-irb-world-rankings-s11-c216-l0.html'
    response = requests.get(rankings_base_url)
    html = response.text

    if response.status_code == 200:
        soup = BeautifulSoup(html, 'html.parser')

        table = soup.find('table')
        tr = table.find_all('tr')[1:]

        for td in tr:
            rank = td.find('td', class_='tdcol-5').text
            country = td.find('td', class_='tdcol-65').text
            points = td.find('td', class_='tdcol-15').text
            country_info = {
                'rank': rank,
                'country': country,
                'points': points
            }
            rankings.append(country_info)
    else:
        print(response.status_code)

    return print('Rankings retrieved and stored in rankings list')


def insert_rankings():
    # Insert rankings into SQL Server

    sql_query = 'INSERT INTO [Rankings] ([Rank], [Country Name], [Points]) VALUES (?, ?, ?)'

    for rank in rankings:
        params = rank['rank'], rank['country'], rank['points']
        cursor.execute(sql_query, params)
        print('Inserted rank: {}, country {}, points {}'.format(*params))


def update_rankings():
    # Update rankings

    sql_query = 'UPDATE [Rankings] SET [Rank] = ?, [Points] = ? WHERE [Country Name] = ?'

    for rank in rankings:
        params = rank['rank'], rank['points'], rank['country']
        cursor.execute(sql_query, params)
        print('Updated rank: {}, points {}, country {}'.format(*params))


def get_schedule():
    # Scraping function to get RWC schedule
    schedule_base_urls = {
        'Pool A': 'https://www.the-sports.org/rugby-world-cup-pool-1-2023-results-eprd110259.html',
        'Pool B': 'https://www.the-sports.org/rugby-world-cup-pool-2-2023-results-eprd110260.html',
        'Pool C': ' https://www.the-sports.org/rugby-world-cup-pool-3-2023-results-eprd110261.html',
        'Pool D': 'https://www.the-sports.org/rugby-world-cup-pool-4-2023-results-eprd110262.html'
    }
    for pool_name, url in schedule_base_urls.items():
        response = requests.get(url)
        html = response.text

        if response.status_code == 200:
            soup = BeautifulSoup(html, 'html.parser')

            div = soup.find('div', id='rencontres')
            tr = div.find_all('tr')

            for element in tr:
                # Find match date
                date = element.find('h6', class_='daterenc')
                if date:
                    date_text = date.get_text(strip=True)

                # Find teams playing
                td = element.find_all('td', class_='tdcol-33')
                teams = [a.find_next('a').get_text(strip=True)
                         for a in td if a.find_next('a')]

                # Find match location
                td = element.find('td', class_='tdcol-15')
                if td is not None:
                    location = td.text.rfind(' ')
                    if location != -1:
                        location_text = td.text[location+1:]

                # Find match result
                result = element.find('td', class_='tdcol-15 td-center')
                if result is not None:
                    result_text = result.get_text(strip=True)
                    if result_text == '-':
                        result_text = 'Upcoming'

                # Append to schedule
                if len(teams) == 2:
                    team_a, team_b = teams
                    match_info = {
                        'pool': pool_name,
                        'date': date_text,
                        'team_a': team_a,
                        'team_b': team_b,
                        'location': location_text,
                        'result': result_text
                    }
                    schedule.append(match_info)
        else:
            print(response.status_code)

    return print('Schedule retrieved and stored in list schedule')


def insert_schedule():
    # Insert rankings into SQL Server

    sql_query = 'INSERT INTO Schedule ([Date], [Pool], [Team A], [Team B], [Result], [Location]) VALUES (?, ?, ?, ?, ?, ?)'

    for match in schedule:
        cursor.execute(sql_query, match['date'], match['pool'],
                       match['team_a'], match['team_b'], match['result'], match['location'])
        print('Inserted date: {}, pool {}, team a {}, team b {}, result {}, location {}'.format(
            match['date'], match['pool'], match['team_a'], match['team_b'], match['result'], match['location']))


def main():
    # Load list for rankings and schedule
    get_rankings()
    get_schedule()

    # Check if there are rows in [Rankings] or [Schedule]
    # If empty then insert, else update
    count_rankings = cursor.execute('SELECT COUNT(*) FROM [Rankings]')
    row = count_rankings.fetchone()
    count = row[0]

    if count == 0:
        print('0 rows returned. Inserting rankings.')
        insert_rankings()
    else:
        print('{} rows returned. Inserting rankings.'.format(count))
        update_rankings()

    count_schedule = cursor.execute('SELECT COUNT(*) FROM [Schedule]')
    row = count_schedule.fetchone()
    count = row[0]

    if count == 0:
        print('0 rows returned. Inserting schedule.')
        insert_schedule()
    else:
        print('{} rows returned. Inserting schedule.'.format(count))
        # update_schedule()

    cursor.commit()
    cursor.close()
    cnxn.close()


if __name__ == '__main__':
    main()
