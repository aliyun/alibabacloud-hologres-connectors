# Copyright (c) Alibaba Cloud PAI.
# SPDX-License-Identifier: Apache-2.0
# deling.sc

from langchain.schema import Document
from hologres_vector import HologresVector
import time
from langchain.embeddings.huggingface import HuggingFaceEmbeddings
import os

class VectorDB:
    def __init__(self, args, cfg=None):
        model_dir = "/code/embedding_model"
        self.model_name_or_path = os.path.join(model_dir, cfg['embedding']['embedding_model'])
        self.embed = HuggingFaceEmbeddings(model_name=self.model_name_or_path,
                                           model_kwargs={'device': 'cpu'})
        self.query_topk = cfg['query_topk']
        self.vectordb_type = args.vectordb_type
        emb_dim = cfg['embedding']['embedding_dimension']
        print('self.vectordb_type',self.vectordb_type)
        if self.vectordb_type == 'Hologres':
            start_time = time.time()
            connection_string_holo = HologresVector.connection_string_from_db_params(
                host=cfg['HOLOCfg']['PG_HOST'],
                port=cfg['HOLOCfg']['PG_PORT'],
                database=cfg['HOLOCfg']['PG_DATABASE'],
                user=cfg['HOLOCfg']['PG_USER'],
                password=cfg['HOLOCfg']['PG_PASSWORD']
            )
            vector_db = HologresVector(connection_string_holo, emb_dim, 'langchain_demo', table_schema={'page_content': 'text'})
            end_time = time.time()
            print("Connect Hologres success. Cost time: {} s".format(end_time - start_time))
        else:
            assert False, f'Unsupported vectordb type: {self.vectordb_type}'

        self.vectordb = vector_db

    def add_documents(self, docs):
        start_time = time.time()
        embeddings = self.embed.embed_documents([doc.page_content for doc in docs])
        end_time = time.time()
        print(f"Finished embedding of {len(embeddings)} rows, cost time: {end_time - start_time}s")
        schema_datas = [{'page_content': doc.page_content} for doc in docs]
        self.vectordb.upsert_vectors(embeddings, schema_datas=schema_datas, metadatas=[doc.metadata for doc in docs])

    def similarity_search_db(self, query_text, topk):
        assert self.vectordb is not None, f'error: vector db has not been set, please assign a remote type by "--vectordb_type <vectordb>"'
        query_emb = self.embed.embed_query(query_text)
        res = self.vectordb.search(query_emb, k=topk, select_columns=['page_content'])
        res = [Document(page_content=x['page_content'], metadata=x['metadata']) for x in res]
        return res
