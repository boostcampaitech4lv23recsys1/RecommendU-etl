import os
import re
import sys
import json
import requests
import pymysql
from dotenv import load_dotenv

sys.path.append("/opt/ml/github/RecommendU-etl")
from utils import crawling, parsing, make_question_category, make_keyword, make_embedding
from tqdm import tqdm
import pickle
import numpy as np
import pandas as pd
from collections import defaultdict

from selenium import webdriver
import pendulum

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
from pathlib import Path

from googletrans import Translator
from keybert import KeyBERT
from transformers import AutoTokenizer, AutoModel
# ROOT_DIR = "/opt/ml/"

BASE_DIR = "."
load_dotenv()

def crawl_link(**context):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome("/opt/ml/chromedriver", options = options)

    result = crawling.link_crawl(driver)
    context['task_instance'].xcom_push(key = 'urls', value = result)

def check_duplicates(**context):
    urls = context['task_instance'].xcom_pull(key = 'urls')
    new_urls = crawling.drop_duplicates(urls)
    context['task_instance'].xcom_push(key = 'new_urls', value = new_urls)


def crawl_cover_letter(**context):
    """
    driver의 경우, 재선언을 해줘야한다. Selenium.webdriver는 serializable이 불가능하다.
    """
    root_dir = "/opt/ml/airflow_data"
    utc_time = pendulum.now().to_datetime_string()
    

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome("/opt/ml/chromedriver", options = options)
    
    urls = context['task_instance'].xcom_pull(key = 'new_urls')

    crawling.login_protocol(driver)
    
    cnt = 0
    fpath = Path(os.path.join(root_dir, "major_jobkorea_crawl.txt"))
    fpath.parent.mkdir(parents = True, exist_ok = True)
    file_save = open(str(fpath), "w")
    #TODO: :5 지우기
    for url in tqdm(urls[:5]):
        file_save = crawling.self_introduction_crawl(driver, url, file_save)
        
    file_save.close()
    print("[Crawl Success]")


def parse_cover_letter(**context):
    with open("/opt/ml/data/major_raw2small.pkl", 'rb') as f:
        major_raw2small_dict = pickle.load(f)
    with open("/opt/ml/data/major_small2large.pkl", 'rb') as f:
        major_small2large_dict = pickle.load(f)
    with open("/opt/ml/data/job_small2large.pkl", 'rb') as f:
        job_small2large_dict = pickle.load(f)

    processed_dict = {
        'major_raw2small': major_raw2small_dict,
        'major_small2large': major_small2large_dict,
        'job_small2large': job_small2large_dict
    }

    root_dir = "/opt/ml/airflow_data"
    text = open(os.path.join(root_dir, "major_jobkorea_crawl.txt"), 'r')
    text = text.read()

    pattern_document = re.compile(r"<<start>>(.*?)<<end>>", re.DOTALL)
    documents = re.findall(pattern_document, text)

    db = defaultdict(list)
    for n in range(len(documents)):
        db = parsing._preprocess(documents, n, db, processed_dict)
    result = pd.DataFrame(db)
    print("[SUCESS PARSING]")
    result.to_csv("/opt/ml/airflow_data/temp_parsed_data.csv", index = False)



def handle_parsed_data(**context):
    encoder_path = "/opt/ml/github/RecommendU-etl/pickle/feature_categories.pkl"
    with open(encoder_path, 'rb') as f:
        encoder = pickle.load(f)

    df = pd.read_csv("/opt/ml/airflow_data/temp_parsed_data.csv")
    df = parsing.process_company(df)
    df = parsing.add_numbering(df)
    
    df = parsing.encode_db_format(df, encoder)
    document, answer = parsing.split_doc_ans(df)

    document.to_csv("/opt/ml/airflow_data/final_document.csv", index = False)
    answer.to_csv("/opt/ml/airflow_data/new_answer.csv", index = False)
    


def add_question_category(**context):
    question_answer = pd.read_csv("/opt/ml/airflow_data/new_answer.csv") #answer csv로 변경해야 함

    with open("/opt/ml/github/RecommendU-etl/queryset.json") as f:
        QUERY_DICT = json.load(f)

    total_array = {}
    for key in QUERY_DICT.keys():
        key_array = []
        if key == "어려움 극복/목표 달성(성공/실패)":
            for word in QUERY_DICT[key]:
                temp_array = make_question_category.make_achieve_query(question_answer, word)
                key_array.extend(temp_array)
        else:
            for word in QUERY_DICT[key]:
                temp_array = make_question_category.make_query(question_answer, word)
                key_array.extend(temp_array)
        print(f"{key} : {len(set(key_array))}")
        total_array[key] = list(set(key_array))

    cnt=1
    content_array = defaultdict(list)
    for key in total_array.keys():
        array = total_array[key]
        for content in array:
            content_array[content].append(1000000+cnt)
        cnt+=1
    question_answer['question_category'] = question_answer['answer_id'].apply(lambda x: make_question_category.insert_category(content_array, x))

    with open('/opt/ml/airflow_data/question_cate_map_answerid.json', 'r') as f:
        origin_qcategory = json.load(f)

    for row, value in question_answer.iterrows():
        cate = value["question_category"] 
        
        for c in cate:
            c = int(c) - 1000000
            c = str(c)
            a_id = int(value["answer_id"][1:])
            origin_qcategory[c].append(a_id)
    
    with open("/opt/ml/airflow_data/question_cate_map_answerid.json", 'w') as f:
        json.dump(origin_qcategory, f)


    temp = question_answer[['answer_id', 'question_category']]
    temp.to_csv("/opt/ml/airflow_data/processed_ques_categ.csv", index = False)


def add_keyword(**context):
    data = pd.read_csv("/opt/ml/airflow_data/new_answer.csv")
    translator = Translator()
    keybert = KeyBERT()  

    print("[SUCCESS TRANSLATE1]")
    trans, null = make_keyword.answer_translate(data, translator)
    print("[SUCCESS TRANSLATE2]")
    print(f"[NULL]: {null}")
    while null:
        trans, new_null = make_keyword.answer_trans_check(data, trans, null, translator)
        null = new_null
    print("[SUCCESS TRANSLATE3]")

    result, exp_list = make_keyword.answer_keybert(trans, keybert, translator)
    print("[SUCCESS KEYBERT1]")
    while exp_list:
        trans, result, new_exp_list = make_keyword.answer_keybert_check(trans, result, exp_list, keybert, translator)
        exp_list = new_exp_list
    print("[SUCCESS KEYBERT2]")
    
    new_result = []
    for case in result:
        hash_tag = ' '.join(case)
        new_result.append(hash_tag)
    # column: answer_id, answer, summary

    temp = data[['answer_id']]
    temp['summary'] = new_result

    temp.to_csv("/opt/ml/airflow_data/processed_keyword.csv", index = False)


def add_embedding(**context):
    tokenizer = AutoTokenizer.from_pretrained('jhgan/ko-sroberta-multitask')
    model = AutoModel.from_pretrained('jhgan/ko-sroberta-multitask').eval()

    ### answer embedding 추출 ###
    data = pd.read_csv("/opt/ml/airflow_data/new_answer.csv")
    answer_embedding = make_embedding.answer_embedding(tokenizer, model, data["answer"].tolist())

    ### 기존 answer_embedding_matrix 가져와서 추가 ###
    origin_matrix = np.load("/opt/ml/airflow_data/answer_embedding_matrix.npy")
    new_answer_embedding = np.append(origin_matrix, answer_embedding, axis=0)

    np.save("/opt/ml/airflow_data/answer_embedding_matrix", new_answer_embedding)


def merge_dataframe(**context):
    df = pd.read_csv("/opt/ml/airflow_data/new_answer.csv")
    summary = pd.read_csv("/opt/ml/airflow_data/processed_keyword.csv")
    ques_categ = pd.read_csv("/opt/ml/airflow_data/processed_ques_categ.csv")

    df = pd.merge(df, summary, how = 'left', on = 'answer_id')
    df = pd.merge(df, ques_categ, how = 'left', on = 'answer_id')
    
    df.to_csv("/opt/ml/airflow_data/final_answer.csv", index = False)

def send_model(**context):
    to_path = "http://www.recommendu.kro.kr:30001/services/save_embedding/"
    answer_embed = open('/opt/ml/airflow_data/answer_embedding_matrix.npy','rb')
    with open('/opt/ml/airflow_data/question_cate_map_answerid.json', 'r') as f:
        question_answer_map = json.load(f)
    
    question_answer_map = json.dumps(question_answer_map)
    upload = {'answer_embedding':answer_embed}
    jsons = {'question_answer_mapping_json':question_answer_map}
    res = requests.post(to_path, files = upload, data=jsons)
    print('send complete')


def load_to_mysql(**context):
    document = pd.read_csv("/opt/ml/airflow_data/final_document.csv")
    answer = pd.read_csv("/opt/ml/airflow_data/final_answer.csv")

    #document_id,document_url,spec,pro_rating,company_id,job_small_id,major_small_id,school_id
    # doc_id,major_large,major_small,company,job_large,job_small,school,extra_spec,pro_rating,doc_url
    document = document[['doc_id', 'doc_url', 'extra_spec', 'pro_rating', 'company', 'job_small', \
                            'major_small', 'school']]
    
    # answer_id,content,question,user_good_cnt,user_bad_cnt,pro_good_cnt,pro_bad_cnt,summary,view,user_view,document_id,user_impression_cnt
    # doc_id,answer_id,question,answer,doc_url,doc_view,pro_rating,pro_good_cnt,pro_bad_cnt
    answer['user_good_cnt'] = np.zeros(answer.shape[0])
    answer['user_bad_cnt'] = np.zeros(answer.shape[0])
    answer['user_view'] = np.zeros(answer.shape[0])
    answer['user_impression_cnt'] = np.zeros(answer.shape[0])

    answer = answer[['answer_id', 'answer','question', 'user_good_cnt', 'user_bad_cnt', \
                        'pro_good_cnt', 'pro_bad_cnt', 'summary', 'doc_view', 'user_view',\
                        'doc_id', 'user_impression_cnt'
                        ]]

    connect = pymysql.connect(host=os.environ.get('host'), user=os.environ.get('user'), password=os.environ.get('password'), \
                                    db='recommendu',charset='utf8')

    insert_answer_sql = "insert into services_answer (answer_id,content,question,user_good_cnt,user_bad_cnt,pro_good_cnt,pro_bad_cnt,summary,view,user_view,document_id,user_impression_cnt) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    insert_document_sql = "insert into services_document (document_id,document_url,spec,pro_rating,company_id,job_small_id,major_small_id,school_id) values (%s, %s, %s, %s, %s, %s, %s, %s)"
    
    cur = connect.cursor()
    for i in range(len(document)):
        array = list(document.iloc[i])
        print(array)
        cur.execute(insert_document_sql, array)
    connect.commit()

    for i in range(len(answer)):
        array = list(answer.iloc[i])
        cur.execute(insert_answer_sql, array)

    connect.commit()
    connect.close()



local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'hwanseung2',
    'depends_on_past': False,  # 이전 DAG의 Task가 성공, 실패 여부에 따라 현재 DAG 실행 여부가 결정. False는 과거의 실행 결과 상관없이 매일 실행한다
    'start_date': datetime(2023, 2, 8, 9, tzinfo=local_tz), #datetime(2023, 1, 20),
    'retires': 5,
    'retry_delay': timedelta(minutes=5)  # 만약 실패하면 5분 뒤 재실행
}

with DAG(dag_id = 'ETLPipeline', default_args = default_args, schedule_interval = '0 4 * * FRI', tags = ['pipeline']) as dag:
    link_crawling = PythonOperator(
        task_id = 'CrawlingLink',
        python_callable = crawl_link,
        provide_context = True
    )

    load_new_urls = PythonOperator(
        task_id = "DropDuplicates",
        python_callable = check_duplicates,
        provide_context = True
    )

    coverletter_crawling = PythonOperator(
        task_id = 'CrawlingCoverLetter',
        python_callable = crawl_cover_letter,
        provide_context = True
    )

    parse = PythonOperator(
        task_id = 'Parsing',
        python_callable = parse_cover_letter,
        provide_context = True
    )

    handling_data = PythonOperator(
        task_id = 'HandlingParsedData',
        python_callable = handle_parsed_data,
        provide_context = True
    )

    ques_categ = PythonOperator(
        task_id = 'AddQuestionCategory',
        python_callable = add_question_category,
        provide_context = True
    )

    keyword = PythonOperator(
        task_id = 'AddKeyword',
        python_callable = add_keyword,
        provide_context = True
    )

    embed = PythonOperator(
        task_id = 'AddEmbedding',
        python_callable = add_embedding,
        provide_context = True
    )

    dummy = DummyOperator(
        task_id = 'WaitResponse'
    )

    merged = PythonOperator(
        task_id = 'MergeDataFrame',
        python_callable = merge_dataframe,
        provide_context = True
    )

    send = PythonOperator(
        task_id = 'SendEmbedding',
        python_callable = send_model,
        provide_context = True
    )

    load = PythonOperator(
        task_id = 'LoadMySQL',
        python_callable = load_to_mysql,
        provide_context = True
    )


    link_crawling >> load_new_urls >> coverletter_crawling >> parse >> handling_data
    handling_data >> [ques_categ, keyword, embed] >> dummy
    dummy >> [merged, send]
    merged >> load


    # [ques_categ, keyword, embed] >> dummy
    # ques_categ >> dummy
    # keyword >> dummy
    # embed >> dummy