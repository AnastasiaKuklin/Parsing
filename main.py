import pandas as pd
import requests
from bs4 import BeautifulSoup

def get_airport_data(iata_code):
    base_url = "https://en.wikipedia.org"
    url = f"{base_url}/wiki/Lists_of_airports_by_IATA_and_ICAO_code#By_IATA_code"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Найдите ссылку на страницу с соответствующей буквой алфавита
    first_letter = iata_code[0].upper()
    letter_link = None
    for link in soup.find_all('a', href=True):
        if link.text.strip() == first_letter:
            letter_link = link['href']
            break
    
    if not letter_link:
        return None, None, None

    # Перейдите на страницу с IATA-кодами, начинающимися на эту букву
    letter_url = f"{base_url}{letter_link}"
    response = requests.get(letter_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Найдите таблицу с IATA-кодами
    table = soup.find('table', {'class': 'wikitable'})

    # Найдите строку, содержащую нужный IATA-код
    airport_link = None
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if cells and cells[0].get_text(strip=True) == iata_code:
            airport_link = cells[1].find('a')['href']
            break
    
    if not airport_link:
        return None, None, None

    # Перейдите на страницу аэропорта
    airport_url = f"{base_url}{airport_link}"
    response = requests.get(airport_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Найдите таблицу с информацией об аэропорте
    info_table = soup.find('table', {'class': 'infobox'})

    if not info_table:
        return None, None, None

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
            elif 'Statistics as of' in header_text:
                year = cell_text

    return airport_type, passengers, year

def process_excel(input_file, output_file):
    df = pd.read_excel(input_file)

    airport_types = []
    passengers = []
    years = []

    for iata_code in df['IATA']:
        airport_type, passenger_stat, year_stat = get_airport_data(iata_code)
        airport_types.append(airport_type)
        passengers.append(passenger_stat)
        years.append(year_stat)

    df['Airport Type'] = airport_types
    df['Passengers'] = passengers
    df['Year'] = years

    df.to_excel(output_file, index=False)

if __name__ == '__main__':
    input_file = 'input.xlsx'
    output_file = 'output.xlsx'
    process_excel(input_file, output_file)