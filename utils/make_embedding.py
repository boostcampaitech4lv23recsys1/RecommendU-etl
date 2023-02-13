import torch
import numpy as np
import pandas as pd
from transformers import AutoTokenizer, AutoModel

from sklearn.metrics.pairwise import cosine_similarity


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0] #First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

def answer_embedding(tokenizer, model, answer):
    encoded_input = tokenizer(answer, padding = True, truncation = True, return_tensors = 'pt')
    with torch.no_grad():
        output = model(**encoded_input)
        embedding = mean_pooling(output, encoded_input['attention_mask'])
    return embedding

if __name__ == '__main__':
    tokenizer = AutoTokenizer.from_pretrained('jhgan/ko-sroberta-multitask')
    model = AutoModel.from_pretrained('jhgan/ko-sroberta-multitask').eval()

    ### answer embedding 추출 ###
    data = pd.read_csv("/opt/ml/data/test.csv")
    answer_embedding = answer_embedding(tokenizer, model, data["answer"].tolist())

    ### 기존 answer_embedding_matrix 가져와서 추가 ###
    origin_matrix = np.load("/opt/ml/data/answer_embedding_matrix.npy")
    new_answer_embedding = np.append(origin_matrix, answer_embedding, axis=0)

    np.save("/opt/ml/data/answer_embedding_matrix", new_answer_embedding)
