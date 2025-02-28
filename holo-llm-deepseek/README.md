# Hologres+PAI+DeepSeek搭建企业专属问答知识库
- 上传用户本地知识库文件，基于coROM模型生成embedding
- 生成embedding存储到Hologres，并用于后续向量检索
- 输入用户问题，输出该问题的prompt，用于后续PAI-LLM部分生成答案
- 将产生的prompt送入EAS部署的LLM模型服务，实时获取到问题的答案
## Step1  开发环境
安装 3.11 版本以上的 Python，并使用 Anaconda 或 Virtualenv 等工具创建虚拟环境，并执行以下命令安装问答知识库相关依
```bash
pip install -r requirements.txt
```
## Step 2: 配置config.json
- eas_config: 用户已经部署在PAI-EAS上的LLM服务的调用信息，包括url和token, 以及模型推理相关参数
- holo-config: Hologres相关的配置信息。
- embedding: embedding模型路径，可以用户自定义挂载。默认使用'iic/nlp_corom_sentence-embedding_chinese-base'
- query_topk: 知识库向量检索返回的数据条数。
- prompt_template: 用户自定义的`prompt`
## Step 3: 运行main.py
1 不使用知识库,使用PAI-LLM进行对话问答
首次使用会自动下载Embedding模型，需要较长过程
```bash
python main.py -n 
```
2 上传本地知识库到Hologres，完成 embedding
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
什么是 Hologres Serverless Computing, 适用场景有哪些？
PAI-LLM answer:
Hologres Serverless Computing 是一种基于云的计算模型，允许用户在无需管理底层基础设施的情况下运行和扩展计算任务。它支持按需扩展和按使用付费，适用于需要弹性计算资源的场景。

### 适用场景：
1. **实时数据分析**：处理大量实时数据查询，支持OLAP和HTAP，适用于金融监控、物流跟踪等场景。
2. **高并发请求处理**：在短时间内处理大量请求，适合电商促销、社交媒体活动等场景。
3. **弹性扩展**：根据负载自动调整资源，避免资源浪费，适用于预测性维护、动态内容生成等。
4. **按需付费**：只需为实际使用的资源付费，适合预算有限的企业或项目。
5. **快速部署**：简化部署流程，适合需要快速上线的应用，如临时数据分析任务或A/B测试。

Hologres Serverless Computing 通过简化资源管理，提高了效率和响应速度，适用于多种需要弹性计算和快速响应的场景。
```
```bash
python main.py -l
python main.py 
对话结果:
Please enter a Question: 
什么是 Hologres Serverless Computing, 适用场景有哪些？
PAI-LLM + Hologres answer:
Hologres的Serverless Computing是一种计算模式，允许用户将大SQL作业（如CPU或内存消耗大的任务）分配到全托管的资源池中，无需预留固定资源，确保各作业独立运行，避免资源竞争。其适用场景包括：

1. **处理内存溢出问题**：当大SQL作业频繁出现内存溢出错误时，使用Serverless Computing可以提高作业成功率和实例稳定性。
2. **提升资源利用率**：在低峰期资源闲置较多时，Serverless Computing能提升资源利用率并降低成本。
3. **应对高峰期压力**：在高峰期资源紧张，且弹性扩展一倍资源仍无法缓解时，Serverless Computing可进一步增加资源，解决峰值问题。
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