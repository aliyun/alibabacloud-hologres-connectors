import os
import yaml

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


def export_env():
    config_fname = os.path.join(DIR_PATH, 'config', 'config.yaml')
    if not os.path.exists(config_fname):
        print('Please run `python generate_config.py` to generate config file first.')
        exit(0)
    with open(config_fname) as f:
        config = yaml.load(f, yaml.CLoader)
        for k in config:
            print(f'setting env variable {k}: {config[k]}')
            os.environ[k] = config[k]
    os.environ['TOKENIZERS_PARALLELISM'] = 'false'
