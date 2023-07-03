from langchain.chat_models import ChatOpenAI
from langchain.document_loaders.csv_loader import CSVLoader
from langchain.embeddings import ModelScopeEmbeddings
from langchain.schema import (
    BaseMessage,
    HumanMessage,
    SystemMessage
)
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import Hologres

from typing import List
import os
import argparse

from utils import export_env, DIR_PATH


class Chatbot:
    def __init__(self, clear_db: bool = False, no_vector_store: bool = False) -> None:

        with open(os.path.join(DIR_PATH, 'config', 'prompt.txt')) as f:
            self.prompt = f.read()

        self.embeddings = ModelScopeEmbeddings(
            model_id='damo/nlp_corom_sentence-embedding_chinese-base')
        self.chat_history = [SystemMessage(content=self.prompt)]
        self.ai = ChatOpenAI(temperature=0.1)
        self.no_vector_store = no_vector_store
        if no_vector_store:
            print('当前正使用无向量引擎模式，不连接hologres')
        else:
            print('connecting hologres vector store...')
            HOLO_ENDPOINT = os.environ['HOLO_ENDPOINT']
            HOLO_PORT = os.environ['HOLO_PORT']
            HOLO_DATABASE = os.environ['HOLO_DATABASE']
            HOLO_USER = os.environ['HOLO_USER']
            HOLO_PASSWORD = os.environ['HOLO_PASSWORD']

            connection_string = Hologres.connection_string_from_db_params(
                HOLO_ENDPOINT, int(HOLO_PORT), HOLO_DATABASE, HOLO_USER, HOLO_PASSWORD)

            self.vectorstore = Hologres(connection_string=connection_string,
                                        embedding_function=self.embeddings, ndims=768, pre_delete_table=clear_db)

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
        self.vectorstore.add_documents(documents)

    def _generate_context(self, question: str) -> List[BaseMessage]:
        # 将用户查询转换为向量，并搜索相关知识
        if self.no_vector_store:
            docs = []
        else:
            docs = self.vectorstore.similarity_search(question, k=10)
        # 限制context总长度
        current_context_length = 0
        ret = []
        for doc in docs:
            if len(doc.page_content) + current_context_length > 2000:
                continue
            current_context_length += len(doc.page_content)
            ret.append(SystemMessage(content='CONTEXT:\n' + doc.page_content))
        return ret

    def query(self, question: str) -> str:
        self.chat_history.append(HumanMessage(content=question))
        input_messages = self._generate_context(question) + self.chat_history

        ai_response = self.ai(input_messages)
        self.chat_history.append(ai_response)

        return ai_response.content


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='chatbot',
        description='holo chatbot command line interface')
    parser.add_argument('-l', '--load', action='store_true',
                        help='generate embeddings and update the vector database.')
    parser.add_argument('-f', '--files', nargs='*', default=[],
                        help='specify the csv data file to update. If leave empty, all files in ./data will be updated. Only valid when --load is set.')
    parser.add_argument('-c', '--clear', action='store_true',
                        help='clear all data in vector store')
    parser.add_argument('-n', '--no-vector-store', action='store_true',
                        help='run pure ChatGPT without vector store')

    args = parser.parse_args()

    export_env()

    if args.clear:
        print('start with drop vector store')

    bot = Chatbot(args.clear, args.no_vector_store)

    if args.load:
        files = args.files
        if len(files) == 0:
            files = [os.path.join(DIR_PATH, 'data', x)
                     for x in os.listdir(os.path.join(DIR_PATH, 'data'))]
        print(f'start loading files: {files}')
        bot.load_db(files)
        print(f'load finished!')
        exit(0)

    print('下面开始问答，请输入问题。按 ctrl-C 退出')
    while True:
        question = input('Human: ')
        answer = bot.query(question)
        print('Chatbot: ' + answer)
