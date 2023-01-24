import os
from tqdm import tqdm

from selenium import webdriver
from selenium.webdriver.common.by import By


def login_protocol(driver:webdriver.Chrome): # 로그인해야지 로그인창때문에 크롤링 멈추는거 막을 수 있음
    driver.get("https://www.jobkorea.co.kr/")
    driver.find_element(By.XPATH,"/html/body/div[5]/div/div[1]/div[1]/ul/li[1]/button").click()
    driver.find_element(By.ID,"lb_id").send_keys("wazs555")
    driver.find_element(By.ID,"lb_pw").send_keys("wanynu78!")
    driver.find_element(By.XPATH,"/html/body/div[5]/div/div[1]/div[1]/ul/li[1]/div/form/fieldset/div[1]/button").click()
    driver.implicitly_wait(3)

    print("login success")


def self_introduction_crawl(driver:webdriver.Chrome, file_url, file):
    try:
        driver.get(file_url)
        user_info = driver.find_element(By.XPATH,'//*[@id="container"]/div[2]/div[1]/div[1]/h2')
        company = user_info.find_element(By.TAG_NAME,'a')

        season= user_info.find_element(By.TAG_NAME,'em')

        specification=driver.find_element(By.CLASS_NAME,'specLists')
        spec_array = specification.text.split('\n')

        paper = driver.find_element(By.CLASS_NAME,"qnaLists")
        questions = paper.find_elements(By.TAG_NAME,'dt')
        questions_list = []
        for index in questions:
            question = index.find_element(By.CLASS_NAME,'tx')
            if question.text=="":
                index.find_element(By.TAG_NAME,'button').click()
                question = index.find_element(By.CLASS_NAME,'tx')
            questions_list.append(question)

        answers = paper.find_elements(By.TAG_NAME,'dd')
        driver.implicitly_wait(3)
        print(f"[URL]: {file_url}")
        print(f"[COMPANY]: {company.text}")
        print(f"[SEASON]: {season.text}")
        print(f"[SPEC]: {spec_array}")
        print(f"[VIEW]: {spec_array[-2].replace(',', '')}")
        

        file.write(f"<<start>>\n")
        file.write(f"<url>{file_url}</url>\n")
        file.write(f"<company>{company.text}</company>\n")
        file.write(f"<season>{season.text}</season>\n")
        file.write(f"<spec>{spec_array}</spec>\n")
        file.write(f"<view>{spec_array[-2].replace(',', '')}</view>\n")

        for index in range(len(answers)):
            answer =answers[index].find_element(By.CLASS_NAME,'tx')
            if answer.text == "":
                questions[index].find_element(By.TAG_NAME,'button').click()
                answer = answers[index].find_element(By.CLASS_NAME,'tx')
            print(f"[QUESTION - {index + 1}]: {questions_list[index].text}")
            print(f"[ANSWER - {index + 1}]: {answer.text}\n")
            file.write(f"<tag_q>{questions_list[index].text}</tag_q>\n")
            file.write(f"<tag_a>{answer.text}</tag_a>\n")
        print("-" * 250)
        print('\n\n\n')
        
        file.write(f"<<end>>\n")
    
    except Exception as e:
        print("current URL : ", file_url)
        print(f"[ERROR OCCUR URL]: str{e}")
        print("\n\n")

    return file


def main():
    root_dir = "/opt/ml/github/RecommendU-etl/crawling/"

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome("/opt/ml/chromedriver", options = options)

    file = open(os.path.join(root_dir, 'major_jobkorea_link.txt'),'r')
    login_protocol(driver=driver)
    urls = file.readlines()

    cnt = 0
    file_save = open(os.path.join(root_dir, "major_jobkorea_crawl.txt"), "w")
    for url in tqdm(urls):
        preprocessed_url = url.split('?')[0]
        file_save = self_introduction_crawl(driver, preprocessed_url, file_save)
        cnt += 1
        if cnt == 100:
            break
    file_save.close()    


if __name__ == "__main__":
    main()
