"""
d:
cd D:\2020\coding\2020_dead_voters\maricopa_county_property_scraper
env\Scripts\activate.bat
cd D:\2020\coding\ASX_announce_collector
python asx_announce_collector.py

d:
cd D:\2020\coding\ASX_announce_collector
sqlite3 asx_announcements.db

https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/
https://chromedriver.chromium.org/downloads

select distinct stockCode from asx_announce;
select count(*) from year_stockcode_completed;
select count(*) from asx_announce;
select stockCode, count(*) from asx_announce group by stockCode;

select distinct stockCode from code_used_before;
select count(distinct stockCode) from code_used_before;
count(distinct stockCode): 157

select distinct stockCode from code_used_before where stockCode not in (select distinct stockCode from asx_announce);
select count(distinct stockCode) from code_used_before where stockCode not in (select distinct stockCode from asx_announce);
count(distinct stockCode): 157

select * from asx_announce_no_result;
select count(*) from asx_announce_no_result;
select count(distinct stockCode) from asx_announce_no_result;

# TODO:
- check error conditions properly close browser session.
-when no result for stock-year combination - save as a record. (appears to leave a browser session open in this case?)
- when all asx announcements for a stock-year combination recorded, save as a record.
(nb: do not save as a record if run on date partially through current year.)
- check if stock-year combination previously completed.

"""
import winsound
import sys
import os
import glob
import pandas as pd
import numpy as np
import json
import random
#
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
#
import sqlite3
from sqlite3 import Error
from time import sleep
import datetime
import string
from bs4 import BeautifulSoup
import time

sound_on = False
search_url = "https://www.asx.com.au/asx/v2/statistics/announcements.do"
db_local="asx_announcements.db"
conn = sqlite3.connect(db_local)
df_stocks = pd.read_csv (r'20200501-asx-listed-companies.csv')
stockCodes = df_stocks.Code

#year_list = list(range(1,24))
#start_year & end_year count backwards from 0 (o=current year)
start_year = 0
end_year = 20

#driver_filename = [r"D:\chromedriver_98.0.4758.102.exe",r"d:\msedgedriver_win64_ver_99_0_1150_36.exe",r"d:\geckodriver.exe"]
#geckodriver omitted due to driver problems
#driver_filename = [r"D:\chromedriver_98.0.4758.102.exe",r"d:\msedgedriver_win64_ver_99_0_1150_36.exe"]
driver_filename = [r"D:\chromedriver_98.0.4758.102.exe"]

def check_internet():
    cmd = os.system('ping google.com -w 4 > clear')
    if cmd == 0:
        print('Internet is connected')
        return True
    else:
        print('Internet is not connected')
        return False


def get_fresh_driver(driver, search_url):
    print("start get_fresh_driver.")
    restarts_counter = 0
    restart_sleep_duration = 5
    test_driver = False
    if check_internet():
        print("internet connected. proceed.")
    else:
        print("internet not connected. long sleep.")
        sleep(restart_sleep_duration*10)
        print("long sleep completed, retry get_fresh_driver.")
        get_fresh_driver(driver, search_url)
    try:
        while not test_driver:
            print("try close & quit driver, sleep, then reopen.")
            try:
                #driver.close()
                #try quit straight away.
                # driver.close() might error in some cases?
                if driver:
                    print("driver exists, quitting.")
                    driver.close()
                    driver.quit()
                    driver=None
                    sleep(2)
                else:
                    driver=None
            except Exception as e:
                print("\n\nget_fresh_driver:error closing driver.", e)
                print("\n\n")
            #driver_selector = random.randint(0, 3)
            driver_selector = random.randint(0, len(driver_filename))
            driver_selector = 0
            print("randomly selected, driver_selector:", driver_selector)
            #driver_selector=1
            #driver = webdriver.Chrome(executable_path=r"D:\chromedriver.exe")
            if driver_selector==0:
                print("selected chrome driver.")
                #driver = webdriver.Chrome(r"D:\chromedriver.exe")
                driver = webdriver.Chrome(driver_filename[0])
            elif driver_selector==1:
                print("selected edge driver.")
                #driver = webdriver.Edge(r"d:\msedgedriver_ver_93.exe")
                driver = webdriver.Chrome(driver_filename[1])
            else:
                print("selected firefox (gecko) driver.")
                #driver = webdriver.Firefox(executable_path=r"d:/geckodriver.exe")
                driver = webdriver.Chrome(driver_filename[2])
            driver.implicitly_wait(10)
            #print("minimise window.")
            #driver.minimize_window()
            driver.get(search_url)
            test_driver=True
        return driver
    except Exception as e:
        print("\nget_fresh_driver:error closing driver.", e)
        restarts_counter+=1
        print("get_fresh_driver:restarts_counter:{}".format(restarts_counter))
        if driver:
            driver.close()
            driver.quit()
        driver=None
        sleep(restart_sleep_duration)
        get_fresh_driver(driver, search_url)


driver=None
driver = get_fresh_driver(driver, search_url)
print("driver loaded.")


#year_list = list(range(1,24))
year_list = list(range(start_year,end_year))
#year_list.append(1)
year_list.sort()
year_index=0
year_searched=""
stockCode=""
#stockCode = stockCodes[0]
#year_index = year_list[0]

#add code to get df of stock codes & years already completed.
#add code to get list/df of stock codes discontinued.
#nb : refer (table = asx_no_longer_listed, code_used_before, no_announcements)
# table year_stockcode_completed,
#exclude these from screen scraping.
#add code to quit browser on error and test if internet connection is down.

for stockCode in stockCodes:
    try:
        for year_index in year_list:
            try:
                try:
                    cur = conn.cursor()
                    sql_query = "SELECT * FROM year_stockcode_completed WHERE stockCode=? and year_index=?"
                    cur.execute(sql_query, (stockCode, year_index))
                    rows = cur.fetchall()
                    lenrows = len(rows)
                    print("stock code:{} year:{} has {} records.".format(stockCode, year_index, lenrows ))
                except Exception as e:
                    print("first run, table year_stockcode_completed did not exist. e:", e)
                    lenrows=0
                #
                if lenrows==0:
                    print("getting records for : stock code:{} year:{}.".format(stockCode, year_index ))
                    #stockCode = stockCodes[0]
                    #year_index=0
                    driver.get(search_url)
                    print("year_index:", year_index)
                    print("stockCode:", stockCode)
                    issuerCode_elem = driver.find_element_by_id("issuerCode")
                    issuerCode_elem.click()
                    issuerCode_elem.clear()
                    issuerCode_elem.send_keys(stockCode)
                    driver.find_element_by_xpath("//*[@id='timeframeType2']").click()
                    year_select = driver.find_element_by_id("year")
                    year_options = year_select.find_elements_by_tag_name("option")
                    len(year_options)
                    year_searched = year_options[year_index].text
                    year_options[year_index].click()
                    driver.find_element_by_xpath("//*[@id='announcements-search']/div/input").click()
                    #check if results in search results or no.
                    body_html = driver.find_element_by_tag_name("body").get_attribute("innerHTML")
                    if "the company code entered is no longer listed" in body_html:
                        no_longer_listed_dict = {
                            "stockCode":str(stockCode),
                            "year_index":str(year_index),
                            "year_searched":str(year_searched),
                            "date_run":str(datetime.datetime.now().strftime("%Y-%m-%d")),
                            "time_run":str(datetime.datetime.now().strftime("%H:%M:%S")),
                        }
                        print("no_longer_listed_dict:", no_longer_listed_dict)
                        df_temp = pd.DataFrame(no_longer_listed_dict, index=[0])
                        df_temp.to_sql("asx_no_longer_listed", conn, schema=None, index=False, if_exists='append')
                    if "has been used by more than one company in the past" in body_html:
                        code_used_before_dict = {
                            "stockCode":str(stockCode),
                            "year_index":str(year_index),
                            "year_searched":str(year_searched),
                            "date_run":str(datetime.datetime.now().strftime("%Y-%m-%d")),
                            "time_run":str(datetime.datetime.now().strftime("%H:%M:%S")),
                        }
                        print("code_used_before_dict:", code_used_before_dict)
                        df_temp = pd.DataFrame(code_used_before_dict, index=[0])
                        df_temp.to_sql("code_used_before", conn, schema=None, index=False, if_exists='append')
                        #
                        sameid_elems = driver.find_elements_by_class_name("sameid")
                        print("len(sameid_elems):", len(sameid_elems))
                        for sameid_elem in sameid_elems:
                            #sameid_elem = sameid_elems[0]
                            li_elems = sameid_elem.find_elements_by_tag_name("li")
                            prev_name = li_elems[0].text.split("- previously known as")[0].rstrip()
                            for li_elem in li_elems[1:]:
                                ssd_name = li_elem.text
                                ssd_name
                                ssd_names_dict = {
                                    "stockCode":str(stockCode),
                                    "year_index":str(year_index),
                                    "year_searched":str(year_searched),
                                    "prev_name":prev_name,
                                    "ssd_name":ssd_name,
                                    "date_run":str(datetime.datetime.now().strftime("%Y-%m-%d")),
                                    "time_run":str(datetime.datetime.now().strftime("%H:%M:%S")),
                                }
                                print("ssd_names_dict:", ssd_names_dict)
                                df_temp = pd.DataFrame(ssd_names_dict, index=[0])
                                df_temp.to_sql("ssd_names", conn, schema=None, index=False, if_exists='append')
                    if "No announcements were released by" in body_html:
                        no_result_dict = {
                            "stockCode":str(stockCode),
                            "year_index":str(year_index),
                            "year_searched":str(year_searched),
                            "date_run":str(datetime.datetime.now().strftime("%Y-%m-%d")),
                            "time_run":str(datetime.datetime.now().strftime("%H:%M:%S")),
                        }
                        print("no_result_dict:", no_result_dict)
                        df_temp = pd.DataFrame(no_result_dict, index=[0])
                        df_temp.to_sql("asx_announce_no_result", conn, schema=None, index=False, if_exists='append')
                    if "Search results: Company announcements for"  in body_html:
                        results_company_name = driver.find_element_by_xpath("//*[@id='content']/div/h2[1]").text.split("\n")[1]
                        if "No announcements were released by" in body_html:
                            no_announcements_dict = {
                                "stockCode":str(stockCode),
                                "results_company_name":results_company_name,
                                "year_index":str(year_index),
                                "year_searched":str(year_searched),
                                "date_run":str(datetime.datetime.now().strftime("%Y-%m-%d")),
                                "time_run":str(datetime.datetime.now().strftime("%H:%M:%S")),
                            }
                            print("no_announcements_dict:", no_announcements_dict)
                            df_temp = pd.DataFrame(no_announcements_dict, index=[0])
                            df_temp.to_sql("no_announcements", conn, schema=None, index=False, if_exists='append')
                        else:
                            #get data from table
                            tbody_elem = driver.find_element_by_xpath("//*[@id='content']/div/announcement_data/table/tbody")
                            tr_elems = tbody_elem.find_elements_by_tag_name("tr")
                            len(tr_elems)
                            for tr_elem in tr_elems:
                                td_elems = tr_elem.find_elements_by_tag_name("td")
                                date_issued = td_elems[0].text.split("\n")[0]
                                print("date_issued:", date_issued)
                                time_issued = td_elems[0].text.split("\n")[1]
                                print("time_issued:", time_issued)
                                price_sensitive_elem = td_elems[1]
                                if "/images/icon-price-sensitive.svg" in price_sensitive_elem.get_attribute('innerHTML'):
                                    price_sensitive=1
                                else:
                                    price_sensitive=0
                                #<img src="/images/icon-price-sensitive.svg" height="12.5" width="6" class="pricesens" alt="asterix" title="price sensitive">
                                print("price_sensitive:", price_sensitive)
                                announce_title =  td_elems[2].text.split("\n")[0]
                                print("announce_title:", announce_title)
                                #
                                result_dict = {
                                    "stockCode":stockCode,
                                    "results_company_name":results_company_name,
                                    "date_issued":date_issued,
                                    "time_issued":time_issued,
                                    "price_sensitive":price_sensitive,
                                    "announce_title":announce_title,
                                    "date_run":str(datetime.datetime.now().strftime("%Y-%m-%d")),
                                    "time_run":str(datetime.datetime.now().strftime("%H:%M:%S")),
                                }
                                print("result_dict:", result_dict)
                                df_temp = pd.DataFrame(result_dict, index=[0])
                                df_temp.to_sql("asx_announce", conn, schema=None, index=False, if_exists='append')
                    stock_year_dict = {
                        "year_index":str(year_index),
                        "year_searched":str(year_searched),
                        "stockCode":str(stockCode),
                        "date_run":str(datetime.datetime.now().strftime("%Y-%m-%d")),
                        "time_run":str(datetime.datetime.now().strftime("%H:%M:%S")),
                    }
                    df_temp = pd.DataFrame(stock_year_dict, index=[0])
                    df_temp.to_sql("year_stockcode_completed", conn, schema=None, index=False, if_exists='append')
                else:
                    print("records already exist for : stock code:{} year:{}.".format(stockCode, year_index ))
            except Exception as e:
                print("\nerror getting results.", e)
                error_dict = {
                    "year_index":str(year_index),
                    "year_searched":str(year_searched),
                    "stockCode":str(stockCode),
                    "date_run":str(datetime.datetime.now().strftime("%Y-%m-%d")),
                    "time_run":str(datetime.datetime.now().strftime("%H:%M:%S")),
                    "error":str(e),
                }
                print("error_dict:", error_dict)
                df_temp = pd.DataFrame(error_dict, index=[0])
                df_temp.to_sql("error_record", conn, schema=None, index=False, if_exists='append')
                driver=None
                driver = get_fresh_driver(driver, search_url)
                print("driver loaded.")
    except Exception as e:
        print("\nerror getting results.", e)
        error_dict = {
            "year_index":str(year_index),
            "year_searched":str(year_searched),
            "stockCode":str(stockCode),
            "date_run":str(datetime.datetime.now().strftime("%Y-%m-%d")),
            "time_run":str(datetime.datetime.now().strftime("%H:%M:%S")),
            "error":str(e),
        }
        print("error_dict:", error_dict)
        df_temp = pd.DataFrame(error_dict, index=[0])
        df_temp.to_sql("error_record", conn, schema=None, index=False, if_exists='append')
        driver=None
        driver = get_fresh_driver(driver, search_url)
        print("driver loaded.")


print("last stock code scraped, closing driver.")
driver.close()
driver.quit()
driver=None
print("end.")
