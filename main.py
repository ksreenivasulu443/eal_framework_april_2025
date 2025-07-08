import yaml
import os
def read_config():

    config_path = r"C:\Users\Admin\PycharmProjects\eal_framework_april_2025\tests\table3\config.yml"
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    print("==" * 100)
    print("Config data is ", end='\n')
    print(config_data)
    print("==" * 100)
    return config_data

print(read_config()['source']['type'])