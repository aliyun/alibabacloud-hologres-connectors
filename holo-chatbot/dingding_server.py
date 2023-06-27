from bottle import route, request, response, run
import json, time, hmac, hashlib, base64, urllib.parse, os, requests

from chatbot import Chatbot
from utils import export_env

def _get_sign():
    timestamp = str(round(time.time() * 1000))
    secret = os.environ['DINGDING_SECRET']
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc,
                         digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    return timestamp, sign

def send_to_group(msg: str):
    timestamp, sign = _get_sign()
    token = os.environ["DINGDING_TOKEN"]
    url = f'https://oapi.dingtalk.com/robot/send?access_token={token}&timestamp={timestamp}&sign={sign}'
    headers = {'Content-Type': 'application/json;charset=utf-8'}
    data = {
        'msgtype': 'text',
        'text': {
            'content': msg,
        },
    }
    requests.post(url, data=json.dumps(data), headers=headers)

export_env()
bot = Chatbot()

@route('/chat', method='POST')
def do_chat():
    if request.content_type != 'application/json; charset=utf-8':
        return json.dumps({'error': 'Only accept content type: "application/json; charset=utf-8"'})

    message = request.json['text']['content']
    answer = bot.query(message) # 调用机器人

    send_to_group(f'{request.json["senderNick"]}，' + answer)

run(host='localhost', port=8889)