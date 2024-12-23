import yaml
import os

# Load config
def load_config(config_path="config.yml"):
    with open(config_path) as f:
        return yaml.load(f, Loader=yaml.FullLoader)

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
for key in cfg["hdfs"].keys():
    cfg["hdfs"][key] = f"{cfg['hdfs_base_path']}{cfg['hdfs'][key]}"