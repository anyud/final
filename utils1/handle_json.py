import json
import os


def load_db_config(config_file='config\\env.json'):
    base_path = os.path.abspath(os.path.dirname(__file__))
    project_root = os.path.abspath(os.path.join(base_path, os.pardir))  # Đi lên một cấp thư mục từ utils1
    config_file_path = os.path.join(project_root, config_file)
    with open(config_file_path, 'r', encoding='utf-8') as file:
        config = json.load(file)
    return config

