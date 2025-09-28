#!/usr/bin/env python3
"""
Script to upload content from test_data folder to Databricks volume using Databricks CLI.
Always overwrites the destination folder (deletes it first, then uploads).
Supports uploading individual files or entire directories.

Usage:
    python upload_test_data.py                           # Upload entire test_data folder (always overwrites destination)
    python upload_test_data.py sample/test_basic         # Upload only sample/test_basic folder (always overwrites destination)
    python upload_test_data.py table_definitions         # Upload only table_definitions folder (always overwrites destination)
    python upload_test_data.py table_definitions/file.json # Upload specific file (always overwrites destination)
"""

import subprocess
import sys
import os
import argparse
import shlex
from pathlib import Path

# Add the tests directory to the path to allow importing config
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.config import Config


def run_databricks_cli(command: str, profile: str = None):
    """
    Run a Databricks CLI command with optional profile.
    
    Args:
        command: The databricks CLI command to run
        profile: Optional profile name to use
        
    Returns:
        Dict with 'success', 'stdout', 'stderr', 'returncode'
    """
    # Strip whitespace and newlines from command
    command = command.strip()
    
    cmd_parts = ['databricks']
    if profile:
        cmd_parts.extend(['--profile', profile])
    cmd_parts.extend(shlex.split(command))
    
    print(f"🔧 Running: {' '.join(cmd_parts)}")
    
    try:
        result = subprocess.run(
            cmd_parts,
            capture_output=True,
            text=True,
            check=False
        )
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'returncode': result.returncode
        }
    except Exception as e:
        return {
            'success': False,
            'stdout': '',
            'stderr': str(e),
            'returncode': -1
        }


def create_volume_if_not_exists(volume_path: str, profile: str = None):
    """
    Create a Databricks volume if it doesn't exist.
    
    Args:
        volume_path: Path in Databricks volume (e.g., /Volumes/catalog/schema/volume_name)
        profile: Databricks CLI profile to use (optional)
        
    Returns:
        True if volume exists or was created successfully, False otherwise
    """
    print(f"🔍 Checking if volume exists: {volume_path}")
    
    # Parse the volume path to extract catalog, schema, and volume name
    # Expected format: /Volumes/catalog/schema/volume_name
    if not volume_path.startswith('/Volumes/'):
        print(f"❌ Invalid volume path format. Expected: /Volumes/catalog/schema/volume_name")
        return False
    
    path_parts = volume_path.strip('/').split('/')
    if len(path_parts) < 4:
        print(f"❌ Invalid volume path format. Expected: /Volumes/catalog/schema/volume_name")
        return False
    
    catalog = path_parts[1]
    schema = path_parts[2] 
    volume_name = path_parts[3]
    
    print(f"📋 Volume details - Catalog: {catalog}, Schema: {schema}, Volume: {volume_name}")
    
    # First, try to list the volume to see if it exists
    # Add dbfs: prefix for the fs ls command
    dbfs_volume_path = f"dbfs:{volume_path}"
    command = f"fs ls {dbfs_volume_path}"
    result = run_databricks_cli(command, profile)
    
    if result['success']:
        print(f"✅ Volume already exists: {volume_path}")
        return True
    else:
        print(f"📁 Volume does not exist, creating: {volume_path}")
        
        # Create the volume using volumes create command
        print(f"🔧 Creating volume: {catalog}.{schema}.{volume_name}")
        
        # Use databricks volumes create command to create the volume
        command = f"volumes create {catalog} {schema} {volume_name} MANAGED"
        result = run_databricks_cli(command, profile)
        
        if result['success']:
            print(f"✅ Successfully created volume: {volume_path}")
            return True
        else:
            # Check if the error is because volume already exists
            if "already exists" in result['stderr'].lower():
                print(f"✅ Volume already exists: {volume_path}")
                return True
            else:
                print(f"❌ Failed to create volume: {result['stderr']}")
                print(f"💡 You may need to create the volume manually in Databricks UI or ensure you have proper permissions")
                return False


def delete_target_file(target_file_path: str, profile: str = None):
    """
    Delete a specific file in Databricks volume before uploading new content.
    
    Args:
        target_file_path: Path to the file in Databricks volume to delete
        profile: Databricks CLI profile to use (optional)
        
    Returns:
        True if deletion was successful or file didn't exist, False otherwise
    """
    # Strip any whitespace/newlines from the path
    target_file_path = target_file_path.strip()
    
    # Add dbfs: prefix for remote volumes
    if not target_file_path.startswith('dbfs:'):
        target_file_path = f"dbfs:{target_file_path}"
    
    print(f"🗑️  Deleting target file: {target_file_path}")
    
    # Try to delete the file directly
    command = f"fs rm {target_file_path}"
    result = run_databricks_cli(command, profile)
    
    if result['success']:
        print(f"✅ Successfully deleted target file: {target_file_path}")
        return True
    else:
        # Check if the error is because file doesn't exist
        if "no such file" in result['stderr'].lower() or "not found" in result['stderr'].lower():
            print(f"ℹ️  Target file does not exist: {target_file_path}")
            return True
        else:
            print(f"❌ Failed to delete target file: {result['stderr']}")
            return False


def delete_target_folder(target_volume_path: str, profile: str = None):
    """
    Delete the target folder in Databricks volume before uploading new content.
    Only deletes the exact specified folder, nothing outside of it.
    
    Args:
        target_volume_path: Path in Databricks volume to delete
        profile: Databricks CLI profile to use (optional)
        
    Returns:
        True if deletion was successful or folder didn't exist, False otherwise
    """
    # Strip any whitespace/newlines from the path
    target_volume_path = target_volume_path.strip()
    
    # Add dbfs: prefix for remote volumes
    if not target_volume_path.startswith('dbfs:'):
        target_volume_path = f"dbfs:{target_volume_path}"
    
    print(f"🗑️  Deleting target folder: {target_volume_path}")
    print(f"⚠️  This will only delete the exact folder specified, nothing outside of it")
    
    # First check if the folder exists
    command = f"fs ls {target_volume_path}"
    result = run_databricks_cli(command, profile)
    
    if not result['success']:
        print(f"ℹ️  Target folder does not exist or is empty: {target_volume_path}")
        return True
    
    # Delete the folder recursively - this only affects the exact folder specified
    command = f"fs rm --recursive {target_volume_path}"
    result = run_databricks_cli(command, profile)
    
    if result['success']:
        print(f"✅ Successfully deleted target folder: {target_volume_path}")
        return True
    else:
        print(f"❌ Failed to delete target folder: {result['stderr']}")
        return False


def upload_test_data_to_volume(local_path: str, target_volume_path: str, profile: str = None, path: str = None):
    """
    Upload content from test_data folder to Databricks volume.
    Always overwrites the destination folder (deletes it first, then uploads).
    Supports both individual files and directories.
    
    Args:
        local_path: Local path containing test_data to upload
        target_volume_path: Path in Databricks volume
        profile: Databricks CLI profile to use (optional)
        path: Optional path within test_data to upload (e.g., 'sample/test_basic' or 'table_definitions/file.json')
    """
    # Handle path parameter
    if path:
        # Construct the full path to the file/folder
        full_local_path = os.path.join(local_path, path)
        print(f"📤 Uploading from local: {full_local_path}")
        print(f"📁 Target volume: {target_volume_path}")
        print(f"📂 Path: {path}")
        
        # Update local_path to point to the file/folder
        local_path = full_local_path
    else:
        print(f"📤 Uploading from local: {local_path}")
        print(f"📁 Target volume: {target_volume_path}")
    
    # Check if local path exists (this check is already done in main() but keeping for safety)
    if not os.path.exists(local_path):
        print(f"❌ Local path does not exist: {local_path}")
        return False
    
    # Add dbfs: prefix for remote volumes
    if not target_volume_path.startswith('dbfs:'):
        target_volume_path = f"dbfs:{target_volume_path}"
    
    # Upload content (file or directory)
    if path:
        print(f"📤 Uploading content: {path}")
        # Check if it's a file or directory
        if os.path.isfile(local_path):
            # It's a file - delete the target file first, then upload
            target_file_path = f"{target_volume_path.rstrip('/')}/{path}"
            
            print(f"🧹 Cleaning target file before upload: {target_file_path}")
            if not delete_target_file(target_file_path, profile):
                print(f"❌ Failed to clean target file, aborting upload")
                return False
            
            # Create parent directory if it doesn't exist
            parent_dir = os.path.dirname(target_file_path)
            if parent_dir != target_volume_path.rstrip('/'):
                print(f"📁 Creating parent directory: {parent_dir}")
                mkdir_command = f"fs mkdirs {parent_dir}"
                mkdir_result = run_databricks_cli(mkdir_command, profile)
                if not mkdir_result['success']:
                    print(f"❌ Failed to create parent directory: {mkdir_result['stderr']}")
                    return False
            
            command = f"fs cp {local_path} {target_file_path}"
        else:
            # It's a directory - delete the target folder first, then upload
            target_folder_path = f"{target_volume_path.rstrip('/')}/{path}"
            
            print(f"🧹 Cleaning target folder before upload: {target_folder_path}")
            if not delete_target_folder(target_folder_path, profile):
                print(f"❌ Failed to clean target folder, aborting upload")
                return False
            
            command = f"fs cp --recursive {local_path} {target_folder_path}"
    else:
        # Upload entire test_data folder - delete the target folder first
        print(f"🧹 Cleaning target folder before upload...")
        if not delete_target_folder(target_volume_path, profile):
            print(f"❌ Failed to clean target folder, aborting upload")
            return False
        
        print(f"📤 Uploading all content from test_data...")
        command = f"fs cp --recursive {local_path} {target_volume_path}"
    
    result = run_databricks_cli(command, profile)
    
    if result['success']:
        print(f"✅ Successfully uploaded test_data to: {target_volume_path}")
        return True
    else:
        print(f"❌ Failed to upload test_data: {result['stderr']}")
        return False


def list_local_contents(local_path: str):
    """
    List contents of local test_data directory or file.
    
    Args:
        local_path: Local path to list (can be file or directory)
    """
    print(f"📋 Local contents of: {local_path}")
    
    if not os.path.exists(local_path):
        print(f"❌ Local path does not exist: {local_path}")
        return False
    
    if os.path.isfile(local_path):
        # It's a file
        print(f"📄 File: {os.path.basename(local_path)}")
        print(f"📏 Size: {os.path.getsize(local_path)} bytes")
        return True
    elif os.path.isdir(local_path):
        # It's a directory
        print("Contents:")
        for item in os.listdir(local_path):
            item_path = os.path.join(local_path, item)
            if os.path.isdir(item_path):
                print(f"  📁 {item}/")
                # List contents of subdirectories
                try:
                    for subitem in os.listdir(item_path):
                        print(f"    - {subitem}")
                except PermissionError:
                    print(f"    - [Permission denied]")
            else:
                print(f"  📄 {item}")
        return True
    else:
        print(f"❌ Local path is neither a file nor directory: {local_path}")
        return False


def main():
    """Main function to upload test_data to volume."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Upload test data to Databricks volume",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           # Upload entire test_data folder (always overwrites destination)
  %(prog)s sample/test_basic         # Upload only sample/test_basic folder (always overwrites destination)
  %(prog)s table_definitions         # Upload only table_definitions folder (always overwrites destination)
  %(prog)s table_definitions/file.json # Upload specific file (always overwrites destination)
        """
    )
    parser.add_argument(
        'path',
        nargs='?',
        help='Optional path within test_data to upload (e.g., sample/test_basic or table_definitions/file.json)'
    )
    
    args = parser.parse_args()
    
    # Look for config.yml in the tests directory
    config_path = Path(__file__).parent.parent / "config.yml"
    config = Config(str(config_path))
    
    # Get configuration values from config.yml
    target_volume_path = config.get('target.volume').strip() if config.get('target.volume') else None
    target_profile = config.get('target.profile', '').strip()
    
    if not target_volume_path:
        print("❌ Target volume path not specified in config")
        sys.exit(1)
    
    # Local test_data path
    local_path = "./test_data"
    
    print("🚀 Upload Test Data to Databricks Volume")
    print("=" * 50)
    
    if args.path:
        # Check if it's a file or directory
        path_full = os.path.join(local_path, args.path)
        if os.path.isfile(path_full):
            print(f"📄 Uploading file: {args.path}")
        else:
            print(f"📂 Uploading folder: {args.path}")
    else:
        print("📂 Uploading entire test_data folder")
    
    # First, list the local contents
    if args.path:
        # List contents of the specific path
        path_full = os.path.join(local_path, args.path)
        print(f"\n🔍 Checking local path contents...")
        if not list_local_contents(path_full):
            print(f"❌ Cannot access path: {path_full}")
            print("💡 Make sure the path exists and you have read permissions")
            sys.exit(1)
    else:
        # List contents of the entire test_data folder
        print(f"\n🔍 Checking local test_data contents...")
        if not list_local_contents(local_path):
            print(f"❌ Cannot access local path: {local_path}")
            print("💡 Make sure the test_data folder exists and you have read permissions")
            sys.exit(1)
    
    # Then, check and create target volume if it doesn't exist
    print(f"\n🔍 Checking target volume...")
    if not create_volume_if_not_exists(target_volume_path, target_profile):
        print(f"❌ Cannot create or access target volume: {target_volume_path}")
        print("💡 Make sure you have permissions to create volumes in the target workspace")
        sys.exit(1)
    
    # Upload the test_data
    print(f"\n📤 Uploading test_data...")
    success = upload_test_data_to_volume(local_path, target_volume_path, target_profile, args.path)
    
    if success:
        print(f"\n✅ Upload completed successfully!")
        print(f"📁 Test data uploaded to: {target_volume_path}")
    else:
        print(f"\n❌ Upload failed!")
        sys.exit(1)


if __name__ == "__main__":
    main() 