import json
import yaml
import os

# Load config
def load_config(config_path="config.yml"):
    with open(config_path) as f:
        return yaml.load(f, Loader=yaml.FullLoader)

# Save config
def save_config(config_data, config_path="config.yml"):
    with open(config_path, 'w') as f:
        yaml.dump(config_data, f)

cfg = load_config()

# Set env variables on windows
if os.environ.get("mode", "dev") == "dev":
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"

    cfg['spark']['master'] = "spark://localhost:7077"
    # cfg['hdfs_base_path'] = "hdfs://localhost:9000"
elif os.environ.get("mode", "dev") == "prod":
    pass
else:
    raise ValueError("Invalid mode")

# Auto load base path for hdfs
# Loop all hdfs
for key in cfg["hdfs_subpath"].keys():
    if not cfg["hdfs_base_path"].endswith('/') and not cfg["hdfs_subpath"][key].startswith('/'):
        separator = '/'
    else:
        separator = ''
    cfg["hdfs"][key] = f"{cfg['hdfs_base_path']}{separator}{cfg['hdfs_subpath'][key]}"

# Load json flow from .langflow
dict_flowjson = {}
for root, _, files in os.walk(".langflow"):
    for file in files:
        with open(os.path.join(root, file)) as f:
            # key is file name (include .json)
            dict_flowjson[file] = json.load(f)