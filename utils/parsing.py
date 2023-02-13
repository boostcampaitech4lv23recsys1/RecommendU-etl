import os
import re
import pickle
import pandas as pd
from collections import defaultdict

from utils.api import load_document, load_answer

PATTERN_URL = r"<url>(.*?)</url>"
PATTERN_COMPANY = r"<company>(.*?)</company>"
PATTERN_SEASON = r"<season>(.*?)</season>"
PATTERN_SPEC = r"<spec>(.*?)</spec>"
PATTERN_ADVICE_SCORE = r"<advice_score>(.*?)</advice_score>"

PATTERN_QEUSTIONS = re.compile(r"<tag_q>(.*?)</tag_q>", re.DOTALL)
PATTERN_ANSWERS = re.compile(r"<tag_a>(.*?)</tag_a>", re.DOTALL)
PATTERN_GOOD = r"<tag_good>(.*?)</tag_good>"
PATTERN_BAD = r"<tag_bad>(.*?)</tag_bad>"


def _preprocess(documents, n, db, processed_dict):
    doc_url = re.search(PATTERN_URL, documents[n]).group(1)
    company = re.search(PATTERN_COMPANY, documents[n]).group(1)
    
    season_job = re.search(PATTERN_SEASON, documents[n]).group(1) # 2015년 하반기 신입 의사·치과·한의사
    season_job = season_job.split(" ")
    season = season_job[0] + " " + season_job[1]
    job_small = season_job[3]
    job_large = processed_dict['job_small2large'][job_small]
    
    spec = re.search(PATTERN_SPEC, documents[n]).group(1) #['지방4년', '간호학과', '학점 3.9/4.5', '자격증 3개', '수상 1회', '자원봉사 1회', '36,898', '읽음']
    spec = spec[2:-2].split("', '")
    school = spec[0]
    if school == "고졸":
        raw = "기타"
        extra_spec = spec[1:-2]
    else:
        raw = spec[1]
        extra_spec = spec[2:-2]
    
    extra_spec = [season] + extra_spec

    if processed_dict['major_raw2small'].get(raw) != None:
        major_small = processed_dict['major_raw2small'][raw]
    else:
        major_small = "기타"
        
    major_large = processed_dict['major_small2large'][major_small]
    doc_view = spec[-2]
    doc_view = int(''.join(doc_view.split(',')))
    
    doc_score = re.search(PATTERN_ADVICE_SCORE, documents[n])
    if doc_score:
        doc_score = float(doc_score.group(1))
    else:
        doc_score = -1
        
    questions = re.findall(PATTERN_QEUSTIONS, documents[n])
    answers = re.findall(PATTERN_ANSWERS, documents[n])
    tag_goods = re.findall(PATTERN_GOOD, documents[n])
    tag_bads = re.findall(PATTERN_BAD, documents[n])

    for question, answer, tag_good, tag_bad in zip(questions, answers, tag_goods, tag_bads):
        answer = re.sub(r'좋은점 [1-9]', "", answer)
        answer = re.sub(r'아쉬운점 [1-9]', "", answer)
        answer = re.sub(r'글자수 [1-9]*자', '', answer)
        answer = re.sub(r'글자수 \d{1,3}(,\d{3})*자', '', answer)
        answer = re.sub('\d{1,3}(,\d{3})*Byte', '', answer)
        
        tag_good = int(tag_good)
        tag_bad = int(tag_bad)
        
        db['major_large'].append(major_large)
        db['major_small'].append(major_small)
        db['company'].append(company)
        # db['season'].append(season)
        db['job_large'].append(job_large)
        db['job_small'].append(job_small)
        db['school'].append(school)
        db['extra_spec'].append(extra_spec)
        db['pro_rating'].append(doc_score)
        db['question'].append(question)
        db['answer'].append(answer)
        db['doc_url'].append(doc_url)
        db['doc_view'].append(doc_view)
        db['pro_good_cnt'].append(tag_good)
        db['pro_bad_cnt'].append(tag_bad)
    
    return db

def handle_company(x):
    x = ''.join(x.split(' '))
    for case in ['(주)', '(학)', '(유)', '㈜', '\t']:
        x = x.replace(case, '')
    return x


def process_company(df):
    df['company'] = df['company'].apply(handle_company)
    return df


def handle_doc_id(x):
    name = 'd' + format(x, '06')
    return name

def handle_ans_id(x):
    name = 'a' + format(x, '06')
    return name


def add_numbering(df):
    db_document = load_document()
    db_answer = load_answer()

    db_document['new_id'] = db_document['document_id'].apply(lambda x: int(x[1:]))
    max_doc_id = db_document['new_id'].max()
    db_answer['new_id'] = db_answer['answer_id'].apply(lambda x: int(x[1:]))
    max_ans_id = db_answer['new_id'].max()

    grouped = df.groupby('doc_url')
    df['doc_id'] = grouped.ngroup() + max_doc_id + 1
    df['answer_id'] = df.reset_index().index + max_ans_id + 1

    df['doc_id'] = df['doc_id'].apply(handle_doc_id)
    df['answer_id'] = df['answer_id'].apply(handle_ans_id)
    return df


def encode_db_format(df, encoder):
    categories = ['joblarge', 'jobsmall', 'majorlarge', 'majorsmall', 'company', 'school']
    columns = ['job_large', 'job_small', 'major_large', 'major_small', 'company', 'school']

    for name, column in zip(categories, columns):
        #TODO: 먼저 key에 값이 존재하지 않는 경우를 먼저 확인하고, key에 값이 존재하지 않는다면 기타로 바꾼다.
        #TODO: 이후 기타를 포함한 값을 dictionary를 통해서 인코딩한다.
        keys_ = list(encoder[name].keys())
        df[column] = df[column].apply(lambda x: "기타" if x not in keys_ else x)
        df[column] = df[column].map(encoder[name])
    
    return df

def split_doc_ans(df):
    document = df[['doc_id', 'major_large', 'major_small', 'company', 'job_large', 'job_small', 'school', 'extra_spec', 'pro_rating', 'doc_url']]
    answer = df[['doc_id', 'answer_id', 'question', 'answer', 'doc_url', 'doc_view', 'pro_rating', 'pro_good_cnt', 'pro_bad_cnt']]

    document = document.drop_duplicates()
    
    return document, answer

if __name__ == '__main__':
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

    root_dir = "/opt/ml/data/2023-01-25 18:09:30"
    text = open(os.path.join(root_dir, "major_jobkorea_crawl.txt"), 'r')
    text = text.read()

    pattern_document = re.compile(r"<<start>>(.*?)<<end>>", re.DOTALL)
    documents = re.findall(pattern_document, text)

    db = defaultdict(list)
    for n in range(len(documents)):
        db = _preprocess(documents, n, db, processed_dict)
    result = pd.DataFrame(db)
    print("[SUCESS PARSING]")
    result.to_csv("/opt/ml/output/temp_parsed_data.csv", index = False)
    
    result = process_company(result)
    print("[SUCESS HANDLING COMPANY]")
    result = add_numbering(result)
    print("[SUCESS NUMBERING]")
    print(result.shape)

    encoder_path = "/opt/ml/github/RecommendU-etl/pickle/feature_categories.pkl"
    with open(encoder_path, 'rb') as f:
        encoder = pickle.load(f)
    
    

    document, answer = split_doc_ans(result)
    document.to_csv("/opt/ml/output/splitted_document.csv", index = False)
    answer.to_csv("/opt/ml/output/splitted_answer.csv", index = False)
