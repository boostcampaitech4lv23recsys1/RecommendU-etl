from tqdm import tqdm

import jobkorea
from selenium import webdriver

file = open('major_jobkorea_link.txt','r')
driver = webdriver.Chrome("chromedriver")
jobkorea.login_protocol(driver=driver)
urls = file.readlines()

cnt = 0
file_save = open("./major_jobkorea_crawl.txt", "w")
for url in tqdm(urls):
    preprocessed_url = url.split('?')[0]
    file_save = jobkorea.self_introduction_crawl(driver, preprocessed_url, file_save)
    cnt += 1
    if cnt == 100:
        break
file_save.close()