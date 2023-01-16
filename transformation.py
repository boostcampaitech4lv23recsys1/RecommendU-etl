import re
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

QUERY_DICT = {"성장환경":["삶","학창","부모님","별명","몰입","소중한","사건","인생","학교생활","대인관계","생활신조","가정","배경","특성","성장과정","성장 과정"],
             "전공,과목" : ["전공","과목","배경","학과","교육","프로그래밍에"],
             "취미,특기":["취미","특기","좋아하는 일"],
             "성격의 장/단점" : ["보완점","성격","장점","단점","장단점"],
             "역량,강점":["자질","입사를 위해","적임자","전문성","역량","강점","약점","구별","경쟁력","능력","지원분야에","적성"],
             "지원동기":["금융인","지원하게","S/W","선택한 이유","동기","지원동기","지원한 이유","지원하는 이유","되고 싶","지원사유","지원 사유"],
             "입사 후 포부, 계획, 기여하고 싶은 부분" : ["커리어","Career","입사한다면","향후","희망","입사 후","입사후","포부","채용","업무","직무","년 후","년 뒤","미래","꿈","비전","Vision"],
             "프로젝트":["프로젝트", "과제","프로그래밍 경험","개발언어"],
             "사회활동":["사회생활","관계","책임","사회경험","관심과 열정","학업 이외","학업 외","활동","사회활동","봉사"],
             "어려움 극복/목표 달성(성공/실패)" : ["힘이 되는 말","관점을 조정","목표를","성취","실패","어려움","활용","경험기술","자원","교훈","몰두","성과"],
             "의사소통":["커뮤니케이션","희생","소통","설득","대화"],
             "문제해결":["해결","극복","어려움","활용","경험기술","자원","교훈","성과"],
             "팀워크,협업(동아리,팀)" : ["동아리","팀 목표","공동","조직","협력","협업","타인의","갈등"],
             "창의성":["창조적","창의적","혁신을 추구","창의성","아이디어","개선","새로운"],
             "리더쉽 발휘":["리더","주도"],
             "도전":["도전"],
             "자기소개":["키워드","슬로건","PR","단어","자기소개","자기 소개","소개"],
             "가치관" : ["힘이 되는 말","덕목","What makes you move","무엇이 당신을 움직이게 하는지","motto","겸손","행복","만한 책","책 3권","관점을 조정","신뢰","표현","소중하게","기준","배려","좌우명","정직함","가치관","가치","철학","신념","원칙"],
             "사회 현상 및 트렌드(최근 뉴스, 국내외 이슈 견해)":["농협이 하고 있는 일","기술/정보","경쟁회사","인터넷","사회적","트랜드","트렌드","이슈","전략"],
             "인재상, 기업의 핵심 가치, 기업의 이미지":["좋은 회사","기준","인재상","핵심 가치","핵심가치","원칙","이미지","표현"],
             "경력":["경력","지원분야와","수상"],
             "기타":["자유","기타","존경","받고 싶은","게임"]}


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


def main(args):
    cnt = 0
    transformer = Preprocessor(args.root_dir)
    user = transformer._user_preprocess()
    question_answer = transformer._question_answer_preprocess()

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
    
if __name__ == '__main__':
    args = parse_args()
    main(args)