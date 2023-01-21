import jobkorea
from selenium import webdriver
from selenium.webdriver.common.by import By

file = open('jobkorea_link.txt','r')
driver = webdriver.Chrome("chromedriver")
jobkorea.login_protocol(driver=driver)

while True: # 7354개
    file_url = file.readline()
    if file_url == "":
        break
    jobkorea.self_introduction_crawl(driver=driver,file_url=file_url)