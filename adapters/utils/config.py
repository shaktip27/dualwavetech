import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

# Load .env first
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

# Load YAML config and replace env placeholders
def load_config(path="config.yaml"):
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)

    def resolve(value):
        if isinstance(value, str) and "${" in value and "}" in value:
            start = value.find("${") + 2
            end = value.find("}", start)
            env_key = value[start:end]
            return os.environ.get(env_key, value)
        return value

    def recursive_resolve(d):
        for k, v in d.items():
            if isinstance(v, dict):
                recursive_resolve(v)
            else:
                d[k] = resolve(v)
        return d

    return recursive_resolve(cfg)

config = load_config()