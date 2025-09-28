import json
import os


def load_test_metadata():
    """Load test metadata from test_metadata.json file"""
    try:
        # Get the current working directory and navigate to the same folder
        current_dir = os.path.dirname(os.path.abspath(__file__))
        metadata_path = os.path.join(current_dir, "test_metadata.json")
        
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        return metadata
    except Exception as e:
        print(f"❌ Failed to load test metadata: {e}")
        return None


def load_script_metadata():
    """Load script metadata from script_metadata.json file"""
    try:
        # Get the current working directory and navigate to the same folder
        current_dir = os.path.dirname(os.path.abspath(__file__))
        metadata_path = os.path.join(current_dir, "script_metadata.json")
        
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        return metadata
    except Exception as e:
        print(f"❌ Failed to load script metadata: {e}")
        return None


def get_scripts_config():
    """Load and return the scripts configuration from script metadata"""
    script_metadata = load_script_metadata()
    if not script_metadata:
        raise ValueError("Could not load script metadata")
    
    return script_metadata
