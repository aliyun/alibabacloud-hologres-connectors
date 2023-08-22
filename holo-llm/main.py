from typing import Any, List

from langchain.document_loaders.csv_loader import CSVLoader
from langchain.embeddings import ModelScopeEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import Hologres

import requests
from typing import List
import os
import json
import time
import argparse


class LLMChatbot:
    def __init__(self, config,clear_db) -> None:
        self.config = config
        self.embeddings = ModelScopeEmbeddings(
            model_id=self.config['embedding']['model_id'])
        self.vectorstore = self.connect_hologres(clear_db)

    def connect_hologres(self,clear_db):
        print("start connecting")
        HOLO_ENDPOINT = self.config['holo_config']['HOLO_ENDPOINT']
        HOLO_PORT = self.config['holo_config']['HOLO_PORT']
        HOLO_DATABASE = self.config['holo_config']['HOLO_DATABASE']
        HOLO_USER = self.config['holo_config']['HOLO_USER']
        HOLO_PASSWORD = self.config['holo_config']['HOLO_PASSWORD']
        connection_string = Hologres.connection_string_from_db_params(
            HOLO_ENDPOINT, int(HOLO_PORT), HOLO_DATABASE, HOLO_USER, HOLO_PASSWORD)
        vectorstore = Hologres(
            connection_string=connection_string,
            embedding_function=self.embeddings,
            ndims=768,
            pre_delete_table=clear_db)
        return vectorstore

    def load_db(self, files: List[str]) -> None:
        # read docs
        documents = []
        for fname in files:
            loader = CSVLoader(fname)
            documents += loader.load()

        # split docs
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000, chunk_overlap=100)
        documents = text_splitter.split_documents(documents)

        # store embedding in vectorstore
        start_time = time.time()
        self.vectorstore.add_documents(documents)
        end_time = time.time()
        print(
            "Store embedding into Hologres Success.Cost Time: {:.2f}s".format(
                end_time -
                start_time))

    def generate_context(self, question: str,max_context_length: int) -> str:
        docs = self.vectorstore.similarity_search(
            question, k=self.config['query_topk'])
        # Limit the total length of context
        current_context_length = 0
        ret = []
        for doc in docs:
            if len(doc.page_content) + \
                    current_context_length > max_context_length:
                continue
            current_context_length += len(doc.page_content)
            ret.append(doc.page_content)
        return ret

    def post_requests_to_llama2_eas(self, query_prompt: str) -> str:
        url = self.config['eas_config']['url']
        token = self.config['eas_config']['token']
        headers = {
            "Authorization": token,
            'Accept': "*/*",
            "Content-Type": "application/x-www-form-urlencoded;charset=utf-8"
        }
        response = requests.post(
            url=url,
            data=query_prompt.encode('utf8'),
            headers=headers,
            timeout=60000,
        )
        if response.status_code == 200:
            return response.text
        else:
            return ""

    def query(self, question: str, use_holo: bool = True) -> str:
        message_list = self.generate_context(question,1800)
        context = ''
        if use_holo:
            for i in range(len(message_list)):
                pos = message_list[i].find('content:')
                context = context + message_list[i][pos + 9:-1]
        prompt_template = self.config['prompt_template']
        prompt_query = prompt_template.format(
            context=context, question=question)
        start_time = time.time()
        answer = self.post_requests_to_llama2_eas(prompt_query)
        end_time = time.time()
        print(
            "Get response from PAI-EAS Success.Cost Time: {:.2f}s".format(
                end_time -
                start_time))
        
        if len(answer) == 0:
            return "HTTP request to PAI EAS failed."
        pos = answer.find('Helpful answer:')
        if(pos != -1):
            answer = answer[pos + 16 : -1]
            answer = answer.replace('<br/>','')
        return answer


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='chatbot',
        description='holo chatbot command line interface')
    parser.add_argument('-l', '--load', action='store_true',
                        help='generate embeddings and update the vector database.')
    parser.add_argument('-f', '--files', nargs='*', default=[],
                        help='specify the csv data file to update. If leave empty, all files in ./data will be updated. Only valid when --load is set.')
    parser.add_argument('--clear', action='store_true',
                        help='clear all data in vector store')
    parser.add_argument('-n', '--no-vector-store', action='store_true',
                        help='run pure PAI-LLM without vector store')
    parser.add_argument('--config', help='input configuration json file',default='./config/config.json')
    
    args = parser.parse_args()

    if args.config:
        if os.path.exists(args.config):
            with open(args.config) as f:
                config = json.load(f)
                bot = LLMChatbot(config,args.clear)
                if args.load :
                    files = args.files
                    if len(files) == 0:
                        DIR_PATH = os.path.dirname(os.path.realpath(__file__))
                        files = [os.path.join(DIR_PATH, 'data', x)
                                for x in os.listdir(os.path.join(DIR_PATH, 'data'))]
                    print(f'start loading files: {files}')
                    bot.load_db(files)
                    exit(0)
                
                # Start Question
                while True:
                    print("Please enter a Question: ")
                    question = input()
                    if(args.no_vector_store):
                        answer = bot.query(question,False)
                        print('PAI-LLM answer:\n ' + answer)
                    else: 
                        answer = bot.query(question,True)
                        print('PAI-LLM + Hologres answer:\n ' + answer)
        else:
            print(f"{args.config} is not existed.")
    else : 
        print("The config json file must be set.")
