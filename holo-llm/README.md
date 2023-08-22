# 结合PAI-LLM、Hologres、LangChain搭建问答机器人
- 上传用户本地知识库文件，基于coROM模型生成embedding
- 生成embedding存储到Hologres，并用于后续向量检索
- 输入用户问题，输出该问题的prompt，用于后续PAI-LLM部分生成答案
- 将产生的prompt送入EAS部署的LLM模型服务，实时获取到问题的答案
## Step1  开发环境
首先安装 [Anaconda](https://www.anaconda.com/download/) 或其他Python虚拟环境。
```bash
conda create --name llm_py11 python=3.11
conda activate llm_py11
pip install langchain modelscope psycopg2-binary sentence_transformers   bottle requests
```
## Step 2: 配置config.json
- eas_config: 用户已经部署在PAI-EAS上的LLM服务的调用信息，包括url和token。
- holo-config: Hologres相关的配置信息。
- embedding: embedding模型路径，可以用户自定义挂载。默认使用'damo/nlp_corom_sentence-embedding_english-base'
- query_topk: 知识库向量检索返回的数据条数。
- prompt_template: 用户自定义的`prompt`
## Step 3: 运行main.py
1 不使用知识库,使用PAI-LLM进行对话问答
第一次运行时加载embedding模型会慢一点
```bash
python main.py -n 
```
2 上传知识库到Hologres
```bash
python main.py -l
```
3 结合知识库进行对话问答
```bash
python main.py 
```
效果展示
```bash
python main.py  -n
对话结果:
Please enter a Question: 
what is hologres?
PAI-LLM answer:
Hologres is a term used in the context of digital holography, which refers to the holographic image produced by a digital holographic camera. The term "hologres" is derived from the Greek words "holos" meaning "whole" and "graphein" meaning "to record". It refers to the complete or entire holographic image that is recorded by the camera, rather than just a portion of it. Hologres can be used to create three-dimensional images that appear life-like and can be viewed from different angles, providing a more immersive and realistic viewing experience.Please let me know if you need anything else
```
```bash
python main.py -l
python main.py 
对话结果:
Please enter a Question: 
what is hologres?
PAI-LLM + Hologres answer:
Hologres is a one-stop real-time data warehouse independently developed by Alibaba, which supports real-time writing, real-time updating, real-time processing, and real-time analysis of massive data
```
## Step 4: 更多命令
help
```bash
python main.py -h
```
上传知识库前清除已经存在的数据
```bash
python main.py -l --clear
```
使用指定的config文件
```bash
python main.py --config 文件名
```