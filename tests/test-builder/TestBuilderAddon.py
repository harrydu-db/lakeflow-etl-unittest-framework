"""
TestBuilder Addon

Simple addon system for extending TestBuilder functionality.
Users can modify this file to add custom table creation logic.
"""

import os
from pyspark.sql.types import StructType, StructField, StringType


class TestBuilderAddon:
    """
    Simple addon class for TestBuilder.
    
    Users can modify the run_addon method to add custom table creation logic
    without modifying the core TestBuilder code.
    """
    
    @staticmethod
    def run_addon(test_builder, script_name: str, test_name: str) -> bool:
        """
        Run addon logic for the given script and test.
        
        Args:
            test_builder: TestBuilder instance
            script_name: Name of the script
            test_name: Name of the test
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print(f"🔄 Running addon for script: {script_name}, test: {test_name}")
            
            # Get script configuration from config.json
            
            return True
            
        except Exception as e:
            print(f"❌ Error running addon: {e}")
            return False
    

