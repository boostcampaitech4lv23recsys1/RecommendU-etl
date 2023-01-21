import jobkorea
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import os
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


driver = webdriver.Chrome("./chromedriver")
jobkorea.link_crawl(driver)

"""
file = open('C://data/jobkorea_link.txt','r')
result_path = "C://Users/장윤성/OneDrive - 경북대학교/바탕 화면/my Code/python/boostcamp_crawl/new_jobkorea/"
exception_path = "C://Users/장윤성/OneDrive - 경북대학교/바탕 화면/my Code/python/boostcamp_crawl/jobkorea/"
exception_file = os.listdir(exception_path)
# exception_index = [1016,1944]
# for content in exception_file:
#     exp_tmp = content.split('.')[0]
#     exception_index.append(int(exp_tmp)+1)
result_dic = {}
link_array = []
start= 1 # 하나 담을 때 계속 예전꺼를 하나 지우고 새로 저장. 중간에 오류가 많이 남. 자소서가 없어서. -> 잡코리아가 중간중간 지워서
cnt = 1
while True: # 7398
    file_url = file.readline()
    if file_url == "":
        break
    link_array.append(file_url)
jobkorea.login_protocol(driver=driver)
for i in range(6961,len(link_array)):
    print(i)
    if cnt in exception_index:
        start = cnt+1
        os.remove(result_path+str(cnt-1)+'.tsv')
    else:
        frame = jobkorea.self_introduction_crawl(driver=driver,file_url=link_array[i])
        result_dic[cnt] = frame
        result_frame=pd.DataFrame(result_dic.values(),columns=['url','company','season','spec','questions','answers'])
        result_frame.to_csv(result_path+str(cnt)+".tsv",sep='\t')
        if cnt > start:
            os.remove(result_path+str(cnt-1)+'.tsv')
    cnt+=1
driver.close()

"""