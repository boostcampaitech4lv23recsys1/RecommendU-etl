import json
import pandas as pd
from collections import defaultdict

def make_query(result, string):
    array = []
    for i in range(len(result)):
        answer_id = result.iloc[i]['answer_id']
        question = result.iloc[i]['question']
        if string in question:
            array.append(answer_id)
    return array


def make_achieve_query(result, string):
    array = []
    for i in range(len(result)):
        answer_id = result.iloc[i]['answer_id']
        question = result.iloc[i]['question']
        if string in question:
            if "삼성취업" in question or "특성과" in question:
                pass
            else:
                array.append(answer_id)
    return array

def insert_category(content_array, x):
    if x not in content_array.keys():
        return 'x'
    else:
        return content_array[x]
    
    
if __name__ == '__main__':
    ### question열을 querset으로 맵핑하여 question_category 생성 ###
    question_answer = pd.read_csv("/opt/ml/data/test.csv") #answer csv로 변경해야 함

    with open("../queryset.json") as f:
        QUERY_DICT = json.load(f)

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
            content_array[content].append(1000000+cnt)
        cnt+=1
    question_answer['question_category'] = question_answer['answer_id'].apply(lambda x: insert_category(content_array, x))
    print(question_answer["question_category"][:5])

    ### key: question_category, value: answer_id로 하는 dict 생성 ###
    qcategory = defaultdict(list)
    for row, value in question_answer.iterrows():
        cate = value["question_category"] 
        ### 만약에 csv파일로 불러와서 사용한다면 활용
        # cate = cate[1:-1]
        # print(cate)
        # cate = cate.split(",")
        
        for c in cate:
            c = int(c)
            qcategory[c].append(value["answer_id"])
    
    with open("/opt/ml/data/question_cate_map_answerid.json", 'w') as f:
        json.dump(qcategory, f)
