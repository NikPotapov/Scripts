import codecs
import os
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

column_num = 0
row_num = 0
df = pd.DataFrame(columns=[1, 2, 3])
web_path = input('Please, insert web path:')


def read_chrome_driver(web):
    print('Opening Chrome browser...')
    driver = webdriver.Chrome()
    driver.implicitly_wait(0.5)
    driver.maximize_window()
    driver.get(web)
    return driver


def read_web(driver):
    try:
        print('Identifying password and username...')
        elem = WebDriverWait(driver, 50).until(
            EC.presence_of_element_located((By.ID, 'atlassian-token'))
        )
        n = os.path.join('Page.html')
        f = codecs.open(n, 'w', 'utf−8')
        h = driver.page_source
        f.write(h)
    except ValueError as err:
        print(err)
    finally:
        print('Closing browser..')
        driver.quit()


def define_table_data():
    with open('Page.html', 'r') as f:
        print('Reading page content...')
        contents = f.read()
        soup = BeautifulSoup(contents, 'html.parser')
        selection = soup.find("div", attrs={"class": "wiki-content", "id": "main-content"})
        table = selection.findAll('table', attrs={'class': 'confluenceTable tablesorter tablesorter-default'})
        # print(selection)
    return table


def create_df_into_excel(column_number,
                         row_number,
                         dataframe):
    for tr in define_table_data():
        tds = tr.find_all('td')
        for td in tds:
            if column_number == 3:
                column_number = 0
                row_number = row_number + 1

            column_number = column_number + 1

            dataframe.loc[row_number, column_number] = td.text

    dataframe.rename(columns={1: 'Name',
                              2: 'Old',
                              3: 'New'},
                     inplace=True)

    print('Saving data into output file; Columns:' + str(dataframe.shape[1]) + ', Rows:' + str(dataframe.shape[0]))
    dataframe.to_excel('output.xlsx',
                       sheet_name='Sheet_name_1', index=False)


read_web(read_chrome_driver(web_path))
create_df_into_excel(column_num,
                     row_num,
                     df)
os.remove('Page.html')
print('Done!')