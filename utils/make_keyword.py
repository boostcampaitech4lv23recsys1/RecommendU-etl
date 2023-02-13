
import pickle
from googletrans import Translator
import pandas as pd
from tqdm import tqdm
from keybert import KeyBERT

def answer_translate(data, translator):  
    trans = []
    null_trans = []
    for row, value in tqdm(data.iterrows()):
        try:
            text = translator.translate(value["answer"], src="ko", dest="en")
            text = text.text
            trans.append(text)
        except:
            null_trans.append(row) 
            trans.append("")
    
    return trans, null_trans

def answer_trans_check(data, trans, null_trans, translator):
    new_null = []
    for idx in null_trans:
        try:
            text = translator.translate(data.iloc[idx]["answer"], src="ko", dest="en")
            text = text.text
            trans[idx] = text
        except:
            new_null.append(idx)
    
    return trans, new_null

def answer_keybert(trans, keybert, translator):
    result = []
    exp_list = []
    for idx in range(len(trans)):
        keywords = keybert.extract_keywords(trans[idx], keyphrase_ngram_range=(1, 1), stop_words='english', top_n=5)
        temp = []
        count = 0
        for k in keywords:
            try:
                if count == 3:
                    break
                text = translator.translate(k[0], src="en", dest="ko")
                keyword = text.text
                if (keyword in temp) or (keyword[-1] == "다"):
                    continue
                temp.append('#' + keyword)
                count+=1
            except:
                exp_list.append(idx)
        result.append(temp)
    
    return result, exp_list

def answer_keybert_check(trans, result, exp_list, keybert, translator):
    new_exp_list = []
    for idx in exp_list:
        keywords = keybert.extract_keywords(trans[idx], keyphrase_ngram_range=(1, 1), stop_words='english', top_n=5)
        temp = []
        count = 0
        for k in keywords:
            try:
                if count == 3:
                    break
                text = translator.translate(k[0], src="en", dest="ko")
                keyword = text.text
                if (keyword in temp) or (keyword[-1] == "다"):
                    continue
                temp.append('#' + keyword)
                count+=1
            except:
                new_exp_list.append(idx)
        result[idx] = temp
    
    return trans, result, new_exp_list

def tag_hash(x):
    return_string = ""
    parsed = x.split(',')
    for content in parsed:
        content = content.strip()[1:-1]
        content = '#'+content
        return_string += content + " "
    return return_string


if __name__ == '__main__':
    data = pd.read_csv("/opt/ml/data/test.csv")
    translator = Translator()
    keybert = KeyBERT()  

    trans, null = answer_translate()

    while null:
        trans, new_null = answer_trans_check(trans,null)
        null = new_null

    with open("../pickle/translate.pkl","wb") as f:
        pickle.dump(trans, f)

    result, exp_list = answer_keybert(trans)

    while exp_list:
        trans, result, new_exp_list = answer_keybert_check(trans, result, exp_list)
        exp_list = new_exp_list

    ### result : 태그로 요약된 값
    with open("../pickle/keybert.pkl","wb") as f:
        pickle.dump(result, f)
    
