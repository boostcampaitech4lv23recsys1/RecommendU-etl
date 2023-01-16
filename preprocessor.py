import re
import numpy as np
import pandas as pd

class Preprocessor:
    def __init__(self, data_path):
        self.data_path = data_path
        self.data = pd.read_csv(self.data_path, sep='\t', encoding='utf-8-sig')


    def make_user(self, i):
        str_id = str(i)
        padding = "0" * (6-len(str_id))
        return padding + str_id


    def integer_check(self, char):
        integer_array = [str(i) for i in range(10)]
        if char in integer_array:
            return True
        return False


    def _user_preprocess(self):
        user_dict = {}
        job_category = {}
        for i in range(len(self.data)):
            user_id = self.make_user(i+1)
            company = self.data.iloc[i]['company']
            season_job = self.data.iloc[i]['season'].split(" ")
            spec = self.data.iloc[i]['spec']
            spec=spec[2:-2].split("', '")
            season = season_job[0] +" "+ season_job[1]
            job = season_job[3:][0]
            school = spec[0]
            if school =="고졸":
                major = ""
                extra_spec = spec[1:]
            else:
                major = spec[1]
                extra_spec = spec[2:]
            # print(school,major,extra_spec)
            user_dict[user_id] = [company,season,job,school,major,extra_spec]
            job_category['user_id'] = job_category.get(user_id,"")+job
        result = pd.DataFrame(user_dict.values(),columns=['company','season','job','school','major','extra_spec'])
        result['user_id'] = user_dict.keys()
        return result


    def _question_preprocess(self, data):
        result_array = []
        remove_char1 = ']'
        remove_char2 = ')'
        questions_split = data['questions'].split('!@#')
        for content in questions_split:
            content = content.strip()
            result = ""
            if content[-1] == remove_char1:
                count=-1
                flag =0
                for k in range(len(content[:-1]),-1,-1):
                    count-=1
                    if content[k] == "[":
                        break
                    if self.integer_check(content[k]):
                        flag=1
                if flag == 1:
                    result= content[:count+1]
                else:
                    result = content
                result_array.append(result)
            elif content[-1] == remove_char2:
                count=-1
                flag =0
                for k in range(len(content[:-1]),-1,-1):
                    count-=1
                    if content[k] == "(":
                        break
                    if self.integer_check(content[k]):
                        flag=1
                if flag == 1:
                    result= content[:count+1]
                else:
                    result = content
                result_array.append(result)
            else:
                result_array.append(content)
        return result_array


    def _answer_preprocess(self, data):
        result_array = []
        answers_split = data['answers'].split('!@#')
        for content in answers_split:
            split_by_enter = content.split("\n")
            result="".join(split_by_enter[:-2])
            result = re.sub(r'좋은점 [1-9]', "", result)
            result = re.sub(r'아쉬운점 [1-9]', "", result)
            result_array.append(result)
        return result_array
        

    def _question_answer_preprocess(self):
        question_dict = {}
        cnt=0
        for i in range(len(self.data)):
            data = self.data.iloc[i]
            url = data['url']
            user_id = self.make_user(i+1)
            question = self._question_preprocess(data)
            answer = self._answer_preprocess(data)
            new_question = []
            new_answer = []
            for i in question:
                if i != "":
                    new_question.append(i)
            for i in answer:
                if i !="":
                    new_answer.append(i)
            for i in range(len(answer)):
                question_dict[cnt] = [user_id, self.make_user(cnt+1), question[i], answer[i], url]
                cnt+=1
            question_frame = pd.DataFrame(question_dict.values(), columns = ['user_id','content_id','question','answer','url'])
            question_frame['question'] = question_frame['question'].apply(lambda x:x.replace("!@",""))
        return question_frame