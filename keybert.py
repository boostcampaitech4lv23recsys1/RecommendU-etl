# from keybert import KeyBERT
import pickle
from googletrans import Translator
import pandas as pd
from tqdm import tqdm

# data = pd.read_csv("/opt/ml/data/jk_answers_without_samples_3_4.csv")
data = pd.read_csv("/opt/ml/data/test.csv")
translator = Translator()

def answer_translate():  
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

def answer_trans_check(trans, null_trans):
    for idx in null_trans:
        try:
            text = translator.translate(data.iloc[idx]["answer"], src="ko", dest="en")
            text = text.text
            trans[idx] = text
        except:
            pass
    
    with open("answer_translate.pkl","wb") as f:
        pickle.dump(trans, f)

### for test    
trans, null = answer_translate()
answer_trans_check(trans,null)