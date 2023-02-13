import requests
import json

def send_model(from_path,to_path):
    answer_embed = open(from_path + 'answer_embedding_matrix.npy','rb')
    with open(from_path+'question_cate_map_answerid.json', 'r') as f:
        question_answer_map = json.load(f)
    question_answer_map = json.dumps(question_answer_map)
    upload = {'answer_embedding':answer_embed}
    jsons = {'question_answer_mapping_json':question_answer_map}
    res = requests.post(to_path, files = upload, data=jsons)
    print('send complete')

if __name__ == '__main__':   
    send_model("../pickle/",'http://www.recommendu.kro.kr:30001/services/save_embedding/')