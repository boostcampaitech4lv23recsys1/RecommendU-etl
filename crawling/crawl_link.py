import os
import time

from selenium import webdriver
from selenium.webdriver.common.by import By


def count_pages(driver:webdriver.Chrome) -> int:
    cnt = 1
    driver.get("https://www.jobkorea.co.kr/starter/passassay?schTxt=&Page=1")
    driver.find_element(By.XPATH, "/html/body/div[6]/div/button").click()
    while True:
        print(cnt)
        driver.implicitly_wait(5)
        driver.get("https://www.jobkorea.co.kr/starter/PassAssay?schCType=13&schGroup=&isFilterChecked=1&Page=" + str(cnt))
        driver.implicitly_wait(5)
        pages = driver.find_element(By.XPATH, "/html/body/div[@id='wrap']/div[@id='container']/div[@class='stContainer']/div[@class='starListsWrap ctTarget']/div[@class='tplPagination']")
        driver.implicitly_wait(5)

        if len(pages.find_elements(By.TAG_NAME, 'p')) == 2:
            cnt += 10
            continue
        elif cnt <= 10:
            cnt += 10
            continue
        else:
            print("check")
            print(pages.find_elements(By.TAG_NAME, 'ul'))
            cnt += len(pages.find_elements(By.TAG_NAME, 'li'))
            break
    return cnt


def link_crawl(root_dir: str, driver:webdriver.Chrome):
    array= []
    f = open(os.path.join(root_dir, "major_jobkorea_link.txt"),'w')
    page_count = count_pages(driver)
    print(f"[CHECK PAGE COUNT]: {page_count - 1}")

    for page_num in range(1, page_count):
        driver.get("https://www.jobkorea.co.kr/starter/PassAssay?schCType=13&schGroup=&isFilterChecked=1&Page=" + str(page_num))
        paper_list = driver.find_element(By.XPATH, "/html/body/div[@id='wrap']/div[@id='container']/div[@class='stContainer']/div[@class='starListsWrap ctTarget']/ul")
        print(paper_list)
        driver.implicitly_wait(3)
        urls = paper_list.find_elements(By.TAG_NAME, 'a')
        for url in urls:
            if 'selfintroduction' in url.get_attribute('href'):
                pass
            else:
                array.append(url.get_attribute('href'))
    array = list(set(array))
    print(array)
    for content in array:
        f.write(content+'\n')
    f.close()

def main():
    root_dir = "/opt/ml/github/RecommendU-etl/crawling"

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome("/opt/ml/chromedriver", options = options)
    link_crawl(root_dir, driver)

if __name__ == '__main__':
    main()