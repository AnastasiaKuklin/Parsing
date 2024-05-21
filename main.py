import pandas as pd
import aiohttp
import asyncio
from bs4 import BeautifulSoup

base_url = "https://en.wikipedia.org"

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def get_airport_data(session, iata_code):
    url = f"{base_url}/wiki/Lists_of_airports_by_IATA_and_ICAO_code#By_IATA_code"
    response = await fetch(session, url)
    soup = BeautifulSoup(response, 'html.parser')

    # Найдите ссылку на страницу с соответствующей буквой алфавита
    first_letter = iata_code[0].upper()
    letter_link = None
    iata_section = soup.find('span', {'id': 'By_IATA_code'}).find_next('div', {'class': 'hlist'})
    for link in iata_section.find_all('a', href=True):
        if link.text.strip() == first_letter:
            letter_link = link['href']
            break
    
    if not letter_link:
        return None, None, None, None, None

    # Перейдите на страницу с IATA-кодами, начинающимися на эту букву
    letter_url = f"{base_url}{letter_link}"
    response = await fetch(session, letter_url)
    soup = BeautifulSoup(response, 'html.parser')

    # Найдите таблицу с IATA-кодами
    table = soup.find('table', {'class': 'wikitable sortable'})
    
    airport_name = None
    location = None

    # Найдите строку, содержащую нужный IATA-код
    airport_link = None
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if cells and cells[0].get_text(strip=True) == iata_code:
            airport_link = cells[2].find('a')['href']
            airport_name = cells[2].get_text(strip=True)
            location = cells[3].get_text(strip=True)
            break 
    
    if not airport_link:
        return None, None, None, None, None

    # Перейдите на страницу аэропорта
    airport_url = f"{base_url}{airport_link}"
    response = await fetch(session, airport_url)
    soup = BeautifulSoup(response, 'html.parser')

    # Найдите таблицу с информацией об аэропорте
    info_table = soup.find('table', {'class': 'infobox'})

    if not info_table:
        return None, None, None, airport_name, location

    airport_type = None
    passengers = None
    year = None

    for row in info_table.find_all('tr'):
        header = row.find('th')
        cell = row.find('td')
        if header and cell:
            header_text = header.get_text(strip=True)
            cell_text = cell.get_text(strip=True)
            if header_text == 'Airport type':
                airport_type = cell_text
            elif 'Passengers' in header_text:
                passengers = cell_text
        elif header:
            header_text = header.get_text(strip=True)
            if 'Statistics' in header_text:
                year = header_text
    
    return airport_type, passengers, year, airport_name, location

async def process_excel(input_file, output_file):
    df = pd.read_excel(input_file)

    airport_types = []
    passengers = []
    years = []
    airport_names = []
    locations = []

    async with aiohttp.ClientSession() as session:
        tasks = [get_airport_data(session, iata_code) for iata_code in df['IATA']]
        results = await asyncio.gather(*tasks)

        for airport_type, passenger_stat, year_stat, airport_name, location_stat in results:
            airport_types.append(airport_type)
            passengers.append(passenger_stat)
            years.append(year_stat)
            airport_names.append(airport_name)
            locations.append(location_stat)

    df['Airport Type'] = airport_types
    df['Passengers'] = passengers
    df['Year'] = years
    df['Airport name'] = airport_names
    df['Location'] = locations

    df.to_excel(output_file, index=False)

if __name__ == '__main__':
    input_file = 'input.xlsx'
    output_file = 'output.xlsx'
    asyncio.run(process_excel(input_file, output_file))