

import yaml
from box import Box
from src.text_process import tokenize_text


conf_url = '/var/lib/airflow/dags/config/config_postprocess.yml'
with open(conf_url, 'r') as f:
    config_yaml = yaml.load(f, Loader=yaml.FullLoader)
    config = Box(config_yaml)


def postprocess(config):
    tokenize_text(config.spark,
                  config.db,
                  config.analysis)

postprocess(config)






