from typing import Any, List

from langchain_community.document_loaders import CSVLoader
from langchain_community.embeddings import ModelScopeEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import Hologres

import requests
from typing import List
import os
import json
import time
import argparse


class LLMChatbot:
    def __init__(self, config, clear_db) -> None:
        self.config = config
        self.embeddings = ModelScopeEmbeddings(
            model_id=self.config['embedding']['model_id'])
        self.vectorstore = self.connect_hologres(clear_db)

    def connect_hologres(self, clear_db):
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
            table_name='langchain_embedding',
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

    def generate_context(self, question: str, max_context_length: int) -> str:
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

    def post_requests_to_deepseek_eas(self, query_prompt: str):
        url = self.config['eas_config']['url']
        token = self.config['eas_config']['token']
        stream = True if self.config['eas_config']['stream_mode'] == 1 else False
        temperature = self.config['eas_config']['temperature']
        top_p = self.config['eas_config']['top_p']
        top_k = self.config['eas_config']['top_k']
        max_tokens = self.config['eas_config']['max_tokens']

        headers = {
            "Content-Type": "application/json",
            "Authorization": token,
        }
        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": query_prompt},
        ]
        req = {
            "messages": messages,
            "stream": stream,
            "temperature": temperature,
            "top_p": top_p,
            "top_k": top_k,
            "max_tokens": max_tokens,
        }
        response = requests.post(
            url,
            json=req,
            headers=headers,
            stream=stream,
        )

        if stream:
            for chunk in response.iter_lines(chunk_size=8192, decode_unicode=False):
                msg = chunk.decode("utf-8")
                if msg.startswith("data"):
                    info = msg[6:]
                    if info == "[DONE]":
                        break
                    else:
                        resp = json.loads(info)
                        print(resp["choices"][0]["delta"]
                              ["content"], end="", flush=True)
        else:
            resp = json.loads(response.text)
            print(resp["choices"][0]["message"]["content"])

    def query(self, question: str, use_holo: bool = True):
        message_list = self.generate_context(question, 1800)
        context = ''
        if use_holo:
            for i in range(len(message_list)):
                pos = message_list[i].find('content:')
                context = context + message_list[i][pos + 9:-1]
        prompt_template = self.config['prompt_template']
        prompt_query = prompt_template.format(
            context=context, question=question)
        start_time = time.time()
        answer = self.post_requests_to_deepseek_eas(prompt_query)
        end_time = time.time()
        print("\nGet response from PAI-EAS cost {:.2f} seconds\n".format(
                end_time - start_time))

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
    parser.add_argument(
        '--config', help='input configuration json file', default='./config/config.json')

    args = parser.parse_args()

    if args.config:
        if os.path.exists(args.config):
            with open(args.config) as f:
                config = json.load(f)
                bot = LLMChatbot(config, args.clear)
                if args.load:
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
                    if (args.no_vector_store):
                        print('PAI-LLM answer:\n ')
                        bot.query(question, False)
                    else:
                        print('PAI-LLM + Hologres answer:\n ')
                        bot.query(question, True)
        else:
            print(f"{args.config} is not existed.")
    else:
        print("The config json file must be set.")
