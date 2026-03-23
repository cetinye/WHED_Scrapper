# Source - https://stackoverflow.com/a/74623660
# Posted by João Miranda, modified by community. See post 'Timeline' for change history
# Retrieved 2026-03-23, License - CC BY-SA 4.0

import json
import time
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.select import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def main():
    print("Init")
    driver = init_driver()

    print("Opening Homepage")
    url = "https://www.whed.net/results_institutions.php"
    driver.get(url)
    time.sleep(1)

    print("Gathering Countries")
    countries = get_countries(driver)
    driver.quit()

    print("Scraping")
    start = time.time()
    institution_list = asyncio.run(fetch_all(countries))

    print("Writing out")

    f = open('output.json', 'w')
    f.write(json.dumps(institution_list))
    f.close()
    end = time.time()
    print(f"Total time: {end - start}s")


def init_driver():
    chrome_executable = Service(executable_path='chromedriver.exe', log_path='NUL')
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=chrome_executable, options=chrome_options)
    return driver


def get_countries(driver):
    select = Select(driver.find_element(By.ID, "Chp1"))
    countries = list(map(lambda c: c.get_attribute('value'), select.options))
    countries.pop(0)
    return countries


def extract_institutions(html, country):
    soup = BeautifulSoup(html, 'html.parser')
    page = soup.find('p', {'class': 'infos'}).text
    print(str(page))
    number_of_institutions = str(page).split()[0]
    if number_of_institutions == 'No':
        print(f"No results for {country}")
        return []

    results = []
    inst_index = 0

    raw = soup.find_all('a', {'class': 'fancybox fancybox.iframe'})
    for i in raw:
        results.append({
            'name': str(i.text).strip(),
            'url': 'https://www.whed.net/' + str(i.attrs['href']).strip(),
            'country': country
        })

        inst_index += 1

    return {
        'country': country,
        'count': number_of_institutions,
        'records': results
    }


async def get_institutions(country, session):
    try:
        async with session.post(
            url='https://www.whed.net/results_institutions.php',
            data={"Chp1": country, "nbr_ref_pge": 10000}
        ) as response:
            html = await response.read()
            print(f"Successfully got {country}")
            return extract_institutions(html, country)
    except Exception as e:
        print(f"Unable to get {country} due to {e.__class__}.")


async def fetch_all(countries):
    async with aiohttp.ClientSession() as session:
        return await asyncio.gather(*[get_institutions(country, session) for country in countries])


# Main call
main()
