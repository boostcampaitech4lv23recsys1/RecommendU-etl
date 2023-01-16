import re
import json
import numpy as np
import pandas as pd
import argparse
from collections import defaultdict
from .preprocessor import Preprocessor

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel


import torch
import torch.nn as nn
from transformers import AutoTokenizer, AutoModel, AutoModelForSeq2SeqLM, pipeline



def make_query(result, string):
    array = []
    for i in range(len(result)):
        content_id = result.iloc[i]['content_id']
        question = result.iloc[i]['question']
        if string in question:
            array.append(content_id)
    return array


def make_achieve_query(result, string):
    array = []
    for i in range(len(result)):
        content_id = result.iloc[i]['content_id']
        question = result.iloc[i]['question']
        if string in question:
            if "삼성취업" in question or "특성과" in question:
                pass
            else:
                array.append(content_id)
    return array


def remove_duplicate(data):
    answer = data[["answer"]].values.tolist()
    answer = sum(answer, [])

    tfidf_matrix = TfidfVectorizer().fit_transform(answer)
    cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)
    
    sim_list = []

    for c in range(len(cosine_sim)):
        temp = np.where(cosine_sim[c]>0.9)
        for i in temp[0][1:]:
            if i in sim_list:
                continue       
            qa_df = qa_df.drop(i,axis=0)
            sim_list.append(i)

    return data.reset_index(inplace=True, drop=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Cover-letter transformation"
    )
    parser.add_argument("--root_dir", type=str, help="crawled dataset")
    return parser.parse_args()    


def insert_category(content_array, x):
    if x not in content_array.keys():
        return 'x'
    else:
        return content_array[x]

def summarize_answers(data):
    answers = list(data['answer'])
    model_name = "psyche/KoT5-summarization"
    device = "cuda:0" if torch.cuda.is_available() else "cpu"

    summarizer = pipeline("summarization", model=model_name, tokenizer= model_name, batch_size=16, device = device)

    summary = summarizer(answers, max_length=512)
    summary_result = [x['summary_text'] for x in summary]
    data['summary'] = summary_result
    return data


def add_job_type(job_type : dict, job: str):
    for key, value in job_type.items():
        if job in value:
            return key


def main(args):
    cnt = 0
    transformer = Preprocessor(args.root_dir)
    user = transformer._user_preprocess()
    question_answer = transformer._question_answer_preprocess()

    with open("./queryset.json") as f:
        QUERY_DICT = json.load(f)
    with open("./job_type.json") as f:
        JOB_TYPE = json.load(f)

    total_array = {}
    for key in QUERY_DICT.keys():
        key_array = []
        if key == "어려움 극복/목표 달성(성공/실패)":
            for word in QUERY_DICT[key]:
                temp_array = make_achieve_query(question_answer, word)
                key_array.extend(temp_array)
        else:
            for word in QUERY_DICT[key]:
                temp_array = make_query(question_answer, word)
                key_array.extend(temp_array)
        print(f"{key} : {len(set(key_array))}")
        total_array[key] = list(set(key_array))

    cnt=1
    content_array = defaultdict(list)
    for key in total_array.keys():
        array = total_array[key]
        for content in array:
            content_array[content].append(cnt)
        cnt+=1
    
    question_answer['question_category'] = question_answer['content_id'].apply(lambda x: insert_category(content_array, x))
    question_answer = remove_duplicate(question_answer)

    user["job_type"] = user["job"].apply(lambda x: add_job_type(JOB_TYPE, x))
    
if __name__ == '__main__':
    args = parse_args()
    main(args)