"""
Configuration management for the test framework.
"""

import os
import yaml
from typing import Dict, Any
from dotenv import load_dotenv

class Config:
    """Configuration manager for the test framework."""
    
    def __init__(self, config_path: str = "tests/config.yml"):
        """Initialize configuration from YAML file and environment variables."""
        load_dotenv()
        self.config_path = config_path
        self.config = self._load_config()
        self._substitute_env_vars()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file {self.config_path} not found")
        
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def _substitute_env_vars(self):
        """Substitute environment variables in configuration."""
        def _replace_env_vars(obj):
            if isinstance(obj, dict):
                return {k: _replace_env_vars(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [_replace_env_vars(item) for item in obj]
            elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
                env_var = obj[2:-1]
                return os.getenv(env_var, obj)
            else:
                return obj
        
        self.config = _replace_env_vars(self.config)
    
    @property
    def target_config(self) -> Dict[str, Any]:
        """Get target configuration."""
        return self.config.get('target', {})
    
    def get_target_profile(self) -> str:
        """Get the Databricks CLI profile for target workspace."""
        return self.target_config.get('profile', '')
    
    def get_target_volume(self) -> str:
        """Get the target volume path."""
        return self.target_config.get('volume', '')
    

    
    def get(self, key: str, default=None):
        """Get a configuration value using dot notation."""
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value 