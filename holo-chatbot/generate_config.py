import yaml
import os

print('欢迎使用holo chatbot设置向导，请按提示输入所需密钥信息。')
print('您也可以手动编辑 config/config.yaml 来修改设置')
print()

keys = ['HOLO_ENDPOINT', 'HOLO_PORT', 'HOLO_USER',
        'HOLO_PASSWORD', 'HOLO_DATABASE', 'DINGDING_TOKEN', 'DINGDING_SECRET']

config_file = 'config/config.yaml'

data = {}

if os.path.exists(config_file):
    with open(config_file, 'r') as f:
        data = yaml.load(f, yaml.CLoader)

for key in keys:
    value = input(f'请输入 {key} (按回车跳过): ')
    if key not in data or value != '':
        data[key] = value


with open(config_file, 'w') as f:
    yaml.dump(data, f)
