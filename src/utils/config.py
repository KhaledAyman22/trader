import json
import os
from typing import Dict

def load_config() -> Dict:
    """
    Loads the configuration from config/config.json.
    It constructs an absolute path to the config file based on the location of this script.
    """
  
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(project_root, 'config', 'config.json')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        
    with open(config_path, 'r') as f:
        return json.load(f)