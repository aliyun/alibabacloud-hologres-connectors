# Hologres Chatbot

简单快速地用 Hologres 定制专属聊天机器人

## 简介

目前行业内有两种定制聊天机器人的方式：训练微调（Fine-tuning）与提示词微调（Prompt-tuning）。训练微调是指通过大量数据来训练已有模型，将数据更新体现在大语言模型内的权重中。这种训练耗时长、消耗资源大，并且训练过程充满未知。提供上下文的方法可以使用任意模型作为后端问答API来调用，只是在提问时让系统提供一些相关知识语料，运用大模型的阅读理解与整合能力，做出比语料更贴切的回答。上下文语料与大模型的结合是十分有力的，语料能提供模型没学到过的知识，大模型能给出比语料更加简短、针对问题重点的回答。

我们可以使用任意大模型，例如 ChatGPT 提供的 API。接下来要做的就是从海量语料库中提取出最合适的上下文语料。我们通常会使用向量Embedding + 最近邻搜索的方式，将对语料库建立语义索引，构建一个语料库语义搜索引擎。这样在用户提出问题时，就能根据语义找到最相关的上下文。

本代码样例使用Langchain调用Hologres与ChatGPT，快速用语料定制专家聊天机器人。

### Hologres

Hologres 是行业领先的一站式实时数据仓库引擎，支持海量数据实时写入、实时更新、实时分析。
其向量存储深度集成阿里达摩院的Proxima引擎，能够支持大量、高维向量数据的存储与搜索，提供高性能的向量查询服务。也能与Hologres其他数据类型与索引结合，支持复杂过滤条件的融合查询。

当语料库的规模很大时，建议使用Hologres等专业向量引擎，来降低开发部署成本。

### ChatGPT

ChatGPT带来了大模型的爆火，它是一种人工智能聊天机器人，使用GPT（Generative Pre-trained Transformer）技术，可以进行自然语言处理和生成对话。它可以回答各种问题，提供娱乐、教育和帮助等方面的服务。它生成的自然语言十分流畅，很适合作为答疑机器人回答各种专业问题。

## 安装依赖

首先安装 [Anaconda](https://www.anaconda.com/download/) 或其他Python虚拟环境。

```bash
conda create --name chatbot python=3.11
conda activate chatbot

pip install langchain psycopg2-binary openai sentence_transformers bottle
```

## 获取 openai 密钥

使用ChatGPT，需要先去[官网](https://openai.com/)注册账号，并在[此页面](https://platform.openai.com/account/api-keys)创建OPENAI_API_KEY。

## 准备 Hologres 向量引擎

在[这里](https://www.aliyun.com/product/bigdata/hologram)快速部署一个Hologres实例。

然后，在[控制台](https://hologram.console.aliyun.com/instance)查看连接信息。

然后用下面的命令打开设置向导，填写连接信息（user, password, endpoint, dbname等）：

```bash
python generate_config.py
```

之后，你也可以手动修改 [`config/config.yaml`](./config/config.yaml.example) 来更新密钥信息。

## 生成文字嵌入（text embeddings）

将用于定制的文档资料整理成csv格式（包含title和content两列），放入 `data/` 目录中，例如 [`data/example_data.csv`](./data/example_data.csv)。

然后用下面的命令将数据转换为向量并上传到Hologres。

```bash
python chatbot.py -l
```

## 开始聊天

```bash
python chatbot.py
```

## 运行钉钉机器人服务器

```bash
python dingding_server.py
```

## 更多命令

```bash
# help
python chatbot.py -h

# clear vector database
python chatbot.py -c

# reload vector database
python chatbot.py -c -l

# add new files to existing vector database
python chatbot.py -l -f data/file1.csv data/file2.csv
```
