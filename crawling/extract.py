from tqdm import tqdm

import jobkorea
from selenium import webdriver

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('user-agent={0}'.format(user_agent))

driver = webdriver.Chrome("/opt/ml/chromedriver", options = options)

file = open('major_jobkorea_link.txt','r')
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