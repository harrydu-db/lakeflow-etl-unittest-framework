#!/usr/bin/env python3
"""
Script to download content from Databricks volume to local folder using Databricks CLI.
Supports downloading individual files or entire directories.

Usage:
    python download_test_data.py                           # Download entire source volume to local folder
    python download_test_data.py sample/test_basic         # Download only sample/test_basic folder
    python download_test_data.py table_definitions         # Download only table_definitions folder
    python download_test_data.py table_definitions/file.json # Download specific file
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


def check_volume_exists(volume_path: str, profile: str = None):
    """
    Check if a Databricks volume exists and is accessible.
    
    Args:
        volume_path: Path in Databricks volume (e.g., /Volumes/catalog/schema/volume_name)
        profile: Databricks CLI profile to use (optional)
        
    Returns:
        True if volume exists and is accessible, False otherwise
    """
    print(f"🔍 Checking if volume exists: {volume_path}")
    
    # Add dbfs: prefix for the fs ls command
    dbfs_volume_path = f"dbfs:{volume_path}"
    command = f"fs ls {dbfs_volume_path}"
    result = run_databricks_cli(command, profile)
    
    if result['success']:
        print(f"✅ Volume is accessible: {volume_path}")
        return True
    else:
        print(f"❌ Volume not accessible: {volume_path}")
        print(f"💡 Error: {result['stderr']}")
        return False


def list_volume_contents(volume_path: str, profile: str = None, subpath: str = None):
    """
    List contents of a Databricks volume directory.
    
    Args:
        volume_path: Base path in Databricks volume
        profile: Databricks CLI profile to use (optional)
        subpath: Optional subpath within the volume to list
        
    Returns:
        True if listing was successful, False otherwise
    """
    # Construct the full path
    if subpath:
        full_path = f"{volume_path.rstrip('/')}/{subpath}"
    else:
        full_path = volume_path
    
    # Add dbfs: prefix for the fs ls command
    dbfs_path = f"dbfs:{full_path}"
    command = f"fs ls {dbfs_path}"
    result = run_databricks_cli(command, profile)
    
    if result['success']:
        print(f"📋 Contents of: {full_path}")
        if result['stdout'].strip():
            print(result['stdout'])
        else:
            print("  (empty directory)")
        return True
    else:
        print(f"❌ Failed to list contents: {result['stderr']}")
        return False


def create_local_directory(local_path: str):
    """
    Create local directory if it doesn't exist.
    
    Args:
        local_path: Local directory path to create
        
    Returns:
        True if directory exists or was created successfully, False otherwise
    """
    try:
        os.makedirs(local_path, exist_ok=True)
        print(f"✅ Local directory ready: {local_path}")
        return True
    except Exception as e:
        print(f"❌ Failed to create local directory: {e}")
        return False


def download_from_volume(source_volume_path: str, local_path: str, profile: str = None, subpath: str = None):
    """
    Download content from Databricks volume to local folder.
    Uses a two-step approach: download to temp folder, then process and copy to final destination.
    Automatically handles CSV files by converting directories to single files.
    
    Args:
        source_volume_path: Source path in Databricks volume
        local_path: Local directory path to download to
        profile: Databricks CLI profile to use (optional)
        subpath: Optional subpath within the volume to download (e.g., 'sample/test_basic' or 'table_definitions/file.json')
        
    Returns:
        True if download was successful, False otherwise
    """
    import tempfile
    import shutil
    
    # Handle subpath parameter
    if subpath:
        # Construct the full source path
        full_source_path = f"{source_volume_path.rstrip('/')}/{subpath}"
        print(f"📥 Downloading from volume: {full_source_path}")
        print(f"📁 To local: {local_path}")
        print(f"📂 Subpath: {subpath}")
        
        # Update source path
        source_path = full_source_path
    else:
        print(f"📥 Downloading from volume: {source_volume_path}")
        print(f"📁 To local: {local_path}")
        source_path = source_volume_path
    
    # Add dbfs: prefix for remote volumes
    if not source_path.startswith('dbfs:'):
        source_path = f"dbfs:{source_path}"
    
    # Create temp directory for initial download
    temp_dir = "./temp"
    print(f"📁 Using temp directory: {temp_dir}")
    
    # Remove temp directory if it exists
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    
    # Create temp directory
    os.makedirs(temp_dir, exist_ok=True)
    
    # Download content to temp directory
    if subpath:
        print(f"📥 Downloading content: {subpath}")
        # Check if it's a file or directory by trying to list it
        check_path = source_path.replace('dbfs:', '')
        list_result = list_volume_contents(check_path, profile)
        
        if not list_result:
            print(f"❌ Cannot access source path: {check_path}")
            return False
        
        command = f"fs cp {source_path} {temp_dir}"
    else:
        # Download entire volume
        print(f"📥 Downloading all content from volume...")
        command = f"fs cp --recursive {source_path} {temp_dir}"
    
    result = run_databricks_cli(command, profile)
    
    if not result['success']:
        print(f"❌ Failed to download content: {result['stderr']}")
        return False
    
    print(f"✅ Successfully downloaded content to temp directory")
    
    # Process CSV files in temp directory
    print(f"🔧 Post-processing CSV files in temp directory...")
    process_csv_files(temp_dir)
    
    # Create final local directory
    if not create_local_directory(local_path):
        return False
    
    # Copy processed content to final destination
    print(f"📁 Copying processed content to final destination...")
    try:
        copy_processed_content(temp_dir, local_path)
        
        # Only clean up temp directory after successful processing
        print(f"🗑️  Cleaning up temp directory...")
        shutil.rmtree(temp_dir)
        print(f"✅ Successfully cleaned up temp directory: {temp_dir}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to copy processed content: {e}")
        print(f"⚠️  Temp directory preserved for debugging: {temp_dir}")
        return False


def copy_processed_content(source_dir: str, target_dir: str):
    """
    Copy processed content from source directory to target directory.
    Preserves the directory structure while copying files.
    
    Args:
        source_dir: Source directory containing processed files
        target_dir: Target directory to copy to
    """
    import shutil
    
    print(f"📁 Copying from {source_dir} to {target_dir}")
    
    try:
        # Copy the entire directory tree
        if os.path.exists(target_dir):
            shutil.rmtree(target_dir)
        
        shutil.copytree(source_dir, target_dir)
        print(f"✅ Successfully copied processed content to {target_dir}")
        
    except Exception as e:
        print(f"❌ Failed to copy processed content: {e}")
        raise


def process_csv_files(local_path: str):
    """
    Process downloaded files to convert CSV directories to single CSV files.
    Looks for directories with .csv extension and converts them to single files.
    
    Args:
        local_path: Local directory path to process
    """
    import glob
    import shutil
    
    print(f"🔍 Looking for CSV directories to process...")
    
    # Find all directories that end with .csv
    csv_dirs = []
    for root, dirs, files in os.walk(local_path):
        for dir_name in dirs:
            if dir_name.endswith('.csv'):
                csv_dirs.append(os.path.join(root, dir_name))
    
    if not csv_dirs:
        print(f"ℹ️  No CSV directories found to process")
        return
    
    print(f"📁 Found {len(csv_dirs)} CSV directories to process")
    
    for csv_dir in csv_dirs:
        print(f"🔧 Processing: {csv_dir}")
        
        # Look for part-*.csv files in the directory
        part_files = glob.glob(os.path.join(csv_dir, "part-*.csv"))
        
        if part_files:
            # Store the part file path first (before we potentially remove directories)
            source_part_file = part_files[0]
            
            # Get the parent directory and create the target file name
            parent_dir = os.path.dirname(csv_dir)
            dir_name = os.path.basename(csv_dir)
            # Ensure target_file is a file, not a directory
            target_file = os.path.join(parent_dir, dir_name)
            
            # Convert to absolute paths to avoid any relative path issues
            source_part_file = os.path.abspath(source_part_file)
            target_file = os.path.abspath(target_file)
            
            print(f"  🔍 Source: {source_part_file}")
            print(f"  🔍 Target: {target_file}")
            
            # Use a temporary file name to avoid conflicts
            temp_target_file = target_file + ".tmp"
            
            # Move the part file to a temporary location first
            shutil.move(source_part_file, temp_target_file)
            print(f"  📁 Moved part file to temp: {temp_target_file}")
            
            # Remove the entire CSV directory (including all part files and metadata)
            shutil.rmtree(csv_dir)
            print(f"  🗑️  Removed CSV directory: {csv_dir}")
            
            # Remove target file if it exists
            if os.path.exists(target_file):
                if os.path.isdir(target_file):
                    shutil.rmtree(target_file)
                    print(f"  🗑️  Removed existing directory: {target_file}")
                else:
                    os.remove(target_file)
                    print(f"  🗑️  Removed existing file: {target_file}")
            
            # Now move the temp file to the final target
            shutil.move(temp_target_file, target_file)
            print(f"  ✅ Created single CSV file: {target_file}")
        else:
            print(f"  ⚠️  No part files found in {csv_dir}")


def list_local_contents(local_path: str):
    """
    List contents of local directory.
    
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
        try:
            for item in os.listdir(local_path):
                item_path = os.path.join(local_path, item)
                if os.path.isdir(item_path):
                    print(f"  📁 {item}/")
                    # List contents of subdirectories (limited depth)
                    try:
                        subitems = os.listdir(item_path)
                        for subitem in subitems[:5]:  # Show first 5 items
                            print(f"    - {subitem}")
                        if len(subitems) > 5:
                            print(f"    ... and {len(subitems) - 5} more items")
                    except PermissionError:
                        print(f"    - [Permission denied]")
                else:
                    print(f"  📄 {item}")
        except PermissionError:
            print("  [Permission denied]")
        return True
    else:
        print(f"❌ Local path is neither a file nor directory: {local_path}")
        return False


def main():
    """Main function to download test_data from volume."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Download test data from Databricks volume",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           # Download entire source volume to local folder
  %(prog)s sample/test_basic         # Download only sample/test_basic folder
  %(prog)s table_definitions         # Download only table_definitions folder
  %(prog)s table_definitions/file.json # Download specific file
        """
    )
    parser.add_argument(
        'subpath',
        nargs='?',
        help='Optional subpath within source volume to download (e.g., sample/test_basic or table_definitions/file.json)'
    )
    
    args = parser.parse_args()
    
    # Look for config.yml in the tests directory
    config_path = Path(__file__).parent.parent / "config.yml"
    config = Config(str(config_path))
    
    # Get configuration values from config.yml
    source_volume_path = config.get('source.volume', '').strip()
    source_profile = config.get('source.profile', '').strip()
    local_path = config.get('source.local_path', './test_data_downloaded').strip()
    
    if not source_volume_path:
        print("❌ Source volume path not specified in config")
        sys.exit(1)
    
    print("🚀 Download Test Data from Databricks Volume")
    print("=" * 50)
    
    if args.subpath:
        print(f"📂 Downloading: {args.subpath}")
    else:
        print("📂 Downloading entire source volume")
    
    # First, check if the source volume exists and is accessible
    print(f"\n🔍 Checking source volume...")
    if not check_volume_exists(source_volume_path, source_profile):
        print(f"❌ Cannot access source volume: {source_volume_path}")
        print("💡 Make sure the volume exists and you have read permissions")
        sys.exit(1)
    
    # List the source contents
    print(f"\n🔍 Checking source volume contents...")
    if not list_volume_contents(source_volume_path, source_profile, args.subpath):
        print(f"❌ Cannot list source contents")
        sys.exit(1)
    
    # Download the content
    print(f"\n📥 Downloading content...")
    success = download_from_volume(source_volume_path, local_path, source_profile, args.subpath)
    
    if success:
        print(f"\n✅ Download completed successfully!")
        print(f"📁 Test data downloaded to: {local_path}")
        
        # List the downloaded contents
        print(f"\n🔍 Downloaded contents:")
        list_local_contents(local_path)
    else:
        print(f"\n❌ Download failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
