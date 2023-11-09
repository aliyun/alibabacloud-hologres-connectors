import gradio as gr
from modules.LLMService import LLMService
import time
import os
import json
import sys
import gradio

def html_path(filename):
    script_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    return os.path.join(script_path, "html", filename)

def html(filename):
    path = html_path(filename)
    if os.path.exists(path):
        with open(path, encoding="utf8") as file:
            return file.read()

    return ""

def webpath(fn):
    script_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    if fn.startswith(script_path):
        web_path = os.path.relpath(fn, script_path).replace('\\', '/')
    else:
        web_path = os.path.abspath(fn)
    return f'file={web_path}?{os.path.getmtime(fn)}'

def css_html():
    head = ""
    def stylesheet(fn):
        return f'<link rel="stylesheet" property="stylesheet" href="{webpath(fn)}">'
    
    cssfile = "style.css"
    if not os.path.isfile(cssfile):
        print("cssfile not exist")

    head += stylesheet(cssfile)

    return head

def reload_javascript():
    css = css_html()
    GradioTemplateResponseOriginal = gradio.routes.templates.TemplateResponse

    def template_response(*args, **kwargs):
        res = GradioTemplateResponseOriginal(*args, **kwargs)
        res.body = res.body.replace(b'</body>', f'{css}</body>'.encode("utf8"))
        res.init_headers()
        return res

    gradio.routes.templates.TemplateResponse = template_response
    
def create_ui(service,_global_args,_global_cfg):
    reload_javascript()
    def connect_holo(emb_model, emb_dim, eas_url, eas_token, pg_host, pg_port, pg_database, pg_user, pg_pwd):
        cfg = {
            'embedding': {
                "embedding_model": emb_model,
                "model_dir": "./embedding_model/",
                "embedding_dimension": emb_dim
            },
            'EASCfg': {
                "url": eas_url,
                "token": eas_token
            },
            'HOLOCfg': {
                "PG_HOST": pg_host,
                "PG_DATABASE": pg_database,
                "PG_PORT": int(pg_port),
                "PG_USER": pg_user,
                "PG_PASSWORD": pg_pwd
            },
            "create_docs":{
                "chunk_size": 200,
                "chunk_overlap": 0,
                "docs_dir": "docs/",
                "glob": "**/*"
            }
        }
        _global_args.vectordb_type = "Hologres"
        _global_cfg.update(cfg)
        try:
            service.init_with_cfg(_global_cfg, _global_args)
            return "连接 Hologres 成功"
        except Exception as e:
            return str(e)

    
    with gr.Blocks() as demo:
 
        value_md =  """
            #  <center> \N{fire} Hologres + 大模型搭建企业级问答知识库！ 

            <center>

            \N{rocket} [Hologres产品介绍](https://www.aliyun.com/product/bigdata/hologram)
            / \N{rocket} [Hologres向量计算](https://help.aliyun.com/zh/hologres/user-guide/vector-processing-based-on-proxima)
            / \N{rocket} [Hologres向量Python SDK](https://help.aliyun.com/zh/hologres/developer-reference/vector-computing-sdk)
            / \N{rocket} [PAI-EAS模型在线服务](https://pai.console.aliyun.com)
            / \N{rocket} [通义千问](https://modelscope.cn/models/qwen/Qwen-7B-Chat/summary)

            \N{fire} 请加入[钉钉群聊](https://help.aliyun.com/zh/hologres/support/obtain-online-support-for-hologres)获取更多在线支持
                
            """
        
        
        gr.Markdown(value=value_md)
                
        with gr.Tab("\N{hammer} 设置"):
            with gr.Row():
                with gr.Column():
                    with gr.Column():
                        md_emb = gr.Markdown(value="**请选择 embedding 模型**")
                        emb_model = gr.Dropdown(["text2vec-base-chinese", "SGPT-125M-weightedmean-nli-bitfit"], label="Emebdding Model", value=_global_args.embed_model)
                        emb_dim = gr.Textbox(label="Emebdding Dimension", value=_global_args.embed_dim)
                        def change_emb_model(model):
                            if model == "SGPT-125M-weightedmean-nli-bitfit":
                                return {emb_dim: gr.update(value="768")}
                            if model == "text2vec-base-chinese":
                                return {emb_dim: gr.update(value="768")}
                        emb_model.change(fn=change_emb_model, inputs=emb_model, outputs=[emb_dim])
                    
                    with gr.Column():
                        md_eas = gr.Markdown(value="**请填入模型在线服务PAI-EAS连接信息**")
                        eas_url = gr.Textbox(label="EAS Url", value=_global_cfg['EASCfg']['url'])
                        eas_token = gr.Textbox(label="EAS Token", value=_global_cfg['EASCfg']['token'])
                    
                with gr.Column():
                    md_vs = gr.Markdown(value="**请输入Hologres数据库用户名和密码**")
                    with gr.Column(visible=(_global_cfg['vector_store']=="Hologres")) as holo_col:
                        holo_host = gr.Textbox(label="Host",
                                               value=_global_cfg['HOLOCfg']['PG_HOST'] if _global_cfg['vector_store']=="Hologres" else '')
                        holo_port = gr.Textbox(label="Host",
                                               value=_global_cfg['HOLOCfg']['PG_PORT'] if _global_cfg['vector_store']=="Hologres" else '')
                        holo_database = gr.Textbox(label="Database",
                                                   value=_global_cfg['HOLOCfg']['PG_DATABASE'] if _global_cfg['vector_store']=="Hologres" else '')
                        holo_user = gr.Textbox(label="User",
                                               value=_global_cfg['HOLOCfg']['PG_USER'] if _global_cfg['vector_store']=="Hologres" else '')
                        holo_pwd= gr.Textbox(label="Password",
                                             value=_global_cfg['HOLOCfg']['PG_PASSWORD'] if _global_cfg['vector_store']=="Hologres" else '')
                        connect_btn = gr.Button("连接 Hologres", variant="primary")
                        con_state = gr.Textbox(label="连接信息: ")
                        connect_btn.click(fn=connect_holo, inputs=[emb_model, emb_dim, eas_url, eas_token, holo_host, holo_port, holo_database, holo_user, holo_pwd], outputs=con_state, api_name="connect_holo") 
                    def change_ds_conn(radio):
                        return {holo_col: gr.update(visible=True)}
                
        with gr.Tab("📃 上传"):
            with gr.Row():
                with gr.Column(scale=2):
                    chunk_size = gr.Textbox(label="\N{rocket} 块大小（将文档划分成的块的大小）",value='200')
                    chunk_overlap = gr.Textbox(label="\N{fire} 块重叠大小（相邻文档块彼此重叠的部分）",value='0')
                with gr.Column(scale=8):
                    with gr.Tab("上传文件"):
                        upload_file = gr.File(label="上传知识库文件 (支持的文件类型: txt, md, doc, docx, pdf)",
                                        file_types=['.txt', '.md', '.docx', '.pdf'], file_count="multiple")
                        connect_btn = gr.Button("上传", variant="primary")
                        state_hl_file = gr.Textbox(label="状态")
                        
                    with gr.Tab("上传目录"):
                        upload_file_dir = gr.File(label="上传一个包含知识库文件的文件夹 (支持的文件类型: txt, md, docx, pdf)" , file_count="directory")
                        connect_dir_btn = gr.Button("上传", variant="primary")
                        state_hl_dir = gr.Textbox(label="状态")

                    
                    def upload_knowledge(upload_file,chunk_size,chunk_overlap):
                        file_name = ''
                        for file in upload_file:
                            if file.name.lower().endswith(".txt") or file.name.lower().endswith(".md") or file.name.lower().endswith(".docx") or file.name.lower().endswith(".doc") or file.name.lower().endswith(".pdf"):
                                file_path = file.name
                                file_name += file.name.rsplit('/', 1)[-1] + ', '
                                service.upload_custom_knowledge(file_path,int(chunk_size),int(chunk_overlap))
                        return "成功上传 " + str(len(upload_file)) + " 个文件 [ " +  file_name + "] ! \n \n 相关内容已成功编码并上传至向量数据库，您现在可以开始聊天了！" 
                    
                    def upload_knowledge_dir(upload_dir,chunk_size,chunk_overlap):
                        for file in upload_dir:
                            if file.name.lower().endswith(".txt") or file.name.lower().endswith(".md") or file.name.lower().endswith(".docx") or file.name.lower().endswith(".doc") or file.name.lower().endswith(".pdf"):
                                file_path = file.name
                                service.upload_custom_knowledge(file_path,chunk_size,chunk_overlap)
                        return "成功上传 " + str(len(upload_dir)) + " 个文件!" 

                    connect_btn.click(fn=upload_knowledge, inputs=[upload_file,chunk_size,chunk_overlap], outputs=state_hl_file, api_name="upload_knowledge")
                    connect_dir_btn.click(fn=upload_knowledge_dir, inputs=[upload_file_dir,chunk_size,chunk_overlap], outputs=state_hl_dir, api_name="upload_knowledge_dir")
        
        with gr.Tab("💬 聊天"):
            with gr.Row():
                with gr.Column(scale=2):
                    ds_radio = gr.Radio(
                        [ "向量数据库", "大语言模型", "向量数据库+大语言模型"], label="💬 选择聊天模式"
                    )
                    topk = gr.Textbox(label="查询最相关的k条语料",value='3')
                    with gr.Column():
                        prm_radio = gr.Radio(
                            [ "通用", "URL提取", "自定义"], label="\N{rocket} 请选择prompt模板"
                        )
                        prompt = gr.Textbox(label="Prompt", placeholder="在此处填入prompt模板，上下文和问题用{content}和{question}表示", lines=4)
                        def change_prompt_template(prm_radio):
                            if prm_radio == "通用":
                                return {prompt: gr.update(value="基于以下已知信息，简洁和专业的来回答用户的问题。如果无法从中得到答案，请说 \"根据已知信息无法回答该问题\" 或 \"没有提供足够的相关信息\"，不允许在答案中添加编造成分，答案请使用中文。\n=====\n已知信息:\n{context}\n=====\n用户问题:\n{question}")}
                            elif prm_radio == "URL提取":
                                return {prompt: gr.update(value="你是一位智能小助手，请根据下面我所提供的相关知识，对我提出的问题进行回答。回答的内容必须包括其定义、特征、应用领域以及相关网页链接等等内容，同时务必满足下方所提的要求！\n=====\n 知识库相关知识如下:\n{context}\n=====\n 请根据上方所提供的知识库内容与要求，回答以下问题:\n {question}")}
                            elif prm_radio == "自定义":
                                return {prompt: gr.update(value="")}
                        prm_radio.change(fn=change_prompt_template, inputs=prm_radio, outputs=[prompt])
                        cur_tokens = gr.Textbox(label="\N{fire} 当前token总数")
                with gr.Column(scale=8):
                    chatbot = gr.Chatbot(height=500)
                    msg = gr.Textbox(label="在此处提问")
                    with gr.Row():
                        submitBtn = gr.Button("提交", variant="primary")
                        summaryBtn = gr.Button("总结", variant="primary")
                        clear_his = gr.Button("清空对话", variant="secondary")
                   
                    def respond(message, chat_history, ds_radio, topk, prm_radio, prompt):
                        summary_res = ""
                        if ds_radio == "向量数据库":
                            answer, lens = service.query_only_vectorstore(message,topk)
                        elif ds_radio == "大语言模型":
                            answer, lens, summary_res = service.query_only_llm(message)         
                        else:
                            answer, lens, summary_res = service.query_retrieval_llm(message,topk, prm_radio, prompt)
                        bot_message = answer
                        chat_history.append((message, bot_message))
                        time.sleep(0.05)
                        return "", chat_history, str(lens) + "\n" + summary_res

                    def clear_hisoty(chat_history):
                        chat_history = []
                        service.langchain_chat_history = []
                        service.input_tokens = []
                        # chat_history.append(('Clear the chat history', bot_message))
                        time.sleep(0.05)
                        return chat_history, "0 \n 成功清空!"
                    
                    def summary_hisoty(chat_history):
                        service.input_tokens = []
                        bot_message = service.checkout_history_and_summary(summary=True)
                        chat_history.append(('请对我们之前的对话内容进行总结。', bot_message))
                        tokens_len = service.sp.encode(service.input_tokens, out_type=str)
                        lens = sum(len(tl) for tl in tokens_len)
                        time.sleep(0.05)
                        return chat_history, str(lens) + "\n" + bot_message
                    
                    submitBtn.click(respond, [msg, chatbot, ds_radio, topk, prm_radio, prompt], [msg, chatbot, cur_tokens])
                    clear_his.click(clear_hisoty,[chatbot],[chatbot, cur_tokens])
                    summaryBtn.click(summary_hisoty,[chatbot],[chatbot, cur_tokens])
    
        footer = html("footer.html")
        gr.HTML(footer, elem_id="footer")
        
    return demo