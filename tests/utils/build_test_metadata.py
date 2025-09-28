#!/usr/bin/env python3
"""
Script to analyze lineage information and build dependency graphs for tables and views.
Processes lineage JSON files to find non-volatile tables and their dependent views/tables.
Uses lineage information to determine view/table relationships and build dependency graphs.
"""

import json
import os
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple, Optional
import glob


class MetadataAnalyzer:
    def __init__(self, lineage_dir: str):
        self.lineage_dir = lineage_dir
        self.all_lineage_data = {}
        self.table_view_info = {}  # Maps table_name -> {'is_view': bool, 'is_volatile': bool}
        self.dependency_graph = defaultdict(set)
        self.reverse_dependency_graph = defaultdict(set)
        
    def load_all_lineage_data(self):
        """Load all lineage JSON files and extract table/view information."""
        print("Loading all lineage data...")
        lineage_files = glob.glob(os.path.join(self.lineage_dir, "*_lineage.json"))
        
        for file_path in lineage_files:
            try:
                with open(file_path, 'r') as f:
                    lineage_data = json.load(f)
                
                script_name = lineage_data.get('script_name', '').replace('.sql', '').replace('.sh', '')
                self.all_lineage_data[script_name] = lineage_data
                
                # Extract table/view information from lineage
                tables = lineage_data.get('tables', {})
                for table_name, table_info in tables.items():
                    is_view = table_info.get('is_view', False)
                    is_volatile = table_info.get('is_volatile', True)
                    
                    # If this table already exists, prioritize the view's own lineage file
                    # (i.e., if current is_view=True, don't overwrite with False)
                    if table_name in self.table_view_info:
                        existing_info = self.table_view_info[table_name]
                        # Only update if we're setting is_view=True or if both are False
                        if is_view or not existing_info.get('is_view', False):
                            self.table_view_info[table_name] = {
                                'is_view': is_view,
                                'is_volatile': is_volatile
                            }
                    else:
                        self.table_view_info[table_name] = {
                            'is_view': is_view,
                            'is_volatile': is_volatile
                        }
                    
            except Exception as e:
                print(f"Error loading lineage file {file_path}: {e}")
        
        print(f"Loaded {len(self.all_lineage_data)} lineage files")
        print(f"Found {len(self.table_view_info)} unique tables/views")
    
    def process_lineage_files(self):
        """Process all lineage files to find non-volatile tables and build dependency graph."""
        print("Processing lineage files...")
        
        script_metadata = {}
        
        for script_name, lineage_data in self.all_lineage_data.items():
            tables = lineage_data.get('tables', {})
            
            # Find non-volatile tables (is_volatile: false)
            non_volatile_tables = set()
            for table_name, table_info in tables.items():
                if not table_info.get('is_volatile', True):  # Default to True if not specified
                    non_volatile_tables.add(table_name)
            
            if non_volatile_tables:
                script_metadata[script_name] = {
                    'non_volatile_tables': non_volatile_tables,
                    'all_tables': set(tables.keys())
                }
                
                # Build dependency graph for this script
                self.build_dependency_graph_for_script(script_name, non_volatile_tables)
        
        return script_metadata
    
    def build_dependency_graph_for_script(self, script_name: str, non_volatile_tables: Set[str]):
        """Build dependency graph for a specific script using lineage information."""
        if script_name not in self.all_lineage_data:
            return
            
        lineage_data = self.all_lineage_data[script_name]
        tables = lineage_data.get('tables', {})
        
        # Build dependency graph from lineage information
        for table_name, table_info in tables.items():
            # Get source tables (dependencies)
            sources = table_info.get('source', [])
            for source in sources:
                source_name = source.get('name', '')
                if source_name and source_name != table_name:  # Avoid self-references
                    self.dependency_graph[source_name].add(table_name)
                    self.reverse_dependency_graph[table_name].add(source_name)
            
            # Get target tables (dependents)
            targets = table_info.get('target', [])
            for target in targets:
                target_name = target.get('name', '')
                if target_name and target_name != table_name:  # Avoid self-references
                    self.dependency_graph[table_name].add(target_name)
                    self.reverse_dependency_graph[target_name].add(table_name)
    
    def _find_case_insensitive_key(self, target_name: str, dictionary: dict) -> str:
        """Find a key in dictionary that matches target_name case-insensitively."""
        target_lower = target_name.lower()
        for key in dictionary.keys():
            if key.lower() == target_lower:
                return key
        return target_name  # Return original if not found
    
    def is_view(self, table_name: str) -> bool:
        """Check if a table is a view based on lineage information."""
        key = self._find_case_insensitive_key(table_name, self.table_view_info)
        return self.table_view_info.get(key, {}).get('is_view', False)
    
    def is_volatile(self, table_name: str) -> bool:
        """Check if a table is volatile based on lineage information."""
        key = self._find_case_insensitive_key(table_name, self.table_view_info)
        return self.table_view_info.get(key, {}).get('is_volatile', True)
    
    
    def topological_sort_views(self, views: Set[str]) -> List[str]:
        """Perform topological sort on views to determine dependency order."""
        # Build a subgraph with only the given views
        subgraph = defaultdict(set)
        in_degree = defaultdict(int)
        
        for view in views:
            in_degree[view] = 0
            for dependency in self.reverse_dependency_graph[view]:
                if dependency in views:
                    subgraph[dependency].add(view)
                    in_degree[view] += 1
        
        
        # Topological sort using Kahn's algorithm
        # Sort the initial queue to ensure consistent ordering
        queue = deque(sorted([view for view in views if in_degree[view] == 0]))
        result = []
        
        while queue:
            current = queue.popleft()
            result.append(current)
            
            # Add neighbors in sorted order to maintain consistent ordering
            neighbors = sorted(subgraph[current])
            for neighbor in neighbors:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        return result
    
    
    def get_dependencies_for_script(self, script_name: str) -> Tuple[Set[str], Set[str]]:
        """Get all dependencies (tables and views) for a specific script."""
        if script_name not in self.all_lineage_data:
            return set(), set()
        
        lineage_data = self.all_lineage_data[script_name]
        tables = lineage_data.get('tables', {})
        bteq_statements = lineage_data.get('bteq_statements', [])
        
        all_tables = set()
        all_views = set()
        visited = set()
        
        def process_dependency(item_name):
            """Recursively process dependencies for a table/view."""
            if item_name in visited:
                return
            visited.add(item_name)
            
            if self.is_view(item_name):
                all_views.add(item_name)
                # Find what this view depends on from the view's own lineage data
                if item_name in self.all_lineage_data:
                    view_lineage = self.all_lineage_data[item_name]
                    view_tables = view_lineage.get('tables', {})
                    if item_name in view_tables:
                        view_info = view_tables[item_name]
                        sources = view_info.get('source', [])
                        for source in sources:
                            source_name = source.get('name', '')
                            if source_name and source_name != item_name:
                                # Build dependency graph relationships
                                self.dependency_graph[source_name].add(item_name)
                                self.reverse_dependency_graph[item_name].add(source_name)
                                process_dependency(source_name)
                # Also check in the current script's lineage data
                elif item_name in tables:
                    table_info = tables[item_name]
                    sources = table_info.get('source', [])
                    for source in sources:
                        source_name = source.get('name', '')
                        if source_name and source_name != item_name:
                            # Build dependency graph relationships
                            self.dependency_graph[source_name].add(item_name)
                            self.reverse_dependency_graph[item_name].add(source_name)
                            process_dependency(source_name)
            else:
                all_tables.add(item_name)
        
        # First, process all tables/views mentioned in this script's lineage
        for table_name, table_info in tables.items():
            if not self.is_volatile(table_name):  # Skip volatile tables
                process_dependency(table_name)
        
        
        return all_tables, all_views
    
    def _add_view_base_tables(self, view_name: str, reference_tables: set, visited: set = None):
        """Recursively add base tables that a view depends on to reference_tables."""
        if visited is None:
            visited = set()
        
        if view_name in visited:
            return
        visited.add(view_name)
        
        # Find the case-insensitive key for the view in all_lineage_data
        view_key = self._find_case_insensitive_key(view_name, self.all_lineage_data)
        if view_key in self.all_lineage_data:
            view_lineage = self.all_lineage_data[view_key]
            view_tables = view_lineage.get('tables', {})
            # Find the case-insensitive key for the view in view_tables
            table_key = self._find_case_insensitive_key(view_name, view_tables)
            if table_key in view_tables:
                view_info = view_tables[table_key]
                sources = view_info.get('source', [])
                for source in sources:
                    source_name = source.get('name', '')
                    if source_name and not self.is_volatile(source_name):
                        if not self.is_view(source_name):
                            # It's a base table, add it to reference_tables
                            reference_tables.add(source_name)
                        else:
                            # It's another view, recursively find its base tables
                            self._add_view_base_tables(source_name, reference_tables, visited)
    
    def generate_metadata(self) -> Dict:
        """Generate the final metadata JSON structure."""
        print("Generating metadata...")
        
        # Process lineage files
        script_metadata = self.process_lineage_files()
        
        result = {}
        
        for script_name, script_info in script_metadata.items():
            # Skip if the script name itself is a view
            # Remove .sql extension if present for comparison
            clean_script_name = script_name.replace('.sql', '')
            if self.is_view(clean_script_name):
                continue
            
            # Skip if the script has only one or no bteq_statements (not a real processing script)
            if script_name in self.all_lineage_data:
                bteq_statements = self.all_lineage_data[script_name].get('bteq_statements', [])
                if len(bteq_statements) <= 1:
                    continue
            
            # Get dependencies for this specific script
            all_tables, all_views = self.get_dependencies_for_script(script_name)
            
            # Filter out volatile tables and views
            filtered_tables = {t for t in all_tables if not self.is_volatile(t)}
            filtered_views = {v for v in all_views if not self.is_volatile(v)}
            
            # Sort views using topological sort
            sorted_views = self.topological_sort_views(filtered_views)
            
            # Sort tables alphabetically
            sorted_tables = sorted(filtered_tables)
            
            # For this script, identify which tables are updated vs referenced
            # by looking at the lineage data for this specific script
            update_tables = set()
            reference_tables = set()
            
            if script_name in self.all_lineage_data:
                lineage_data = self.all_lineage_data[script_name]
                tables = lineage_data.get('tables', {})
                
                # Find tables that are targeted (have incoming dependencies from other tables/views)
                for table_name, table_info in tables.items():
                    # Only include actual tables, not views, and exclude volatile tables
                    if not self.is_view(table_name) and not self.is_volatile(table_name):
                        # Check if this table has sources (is being updated by other tables/views)
                        # This means the table is being targeted/updated
                        sources = table_info.get('source', [])
                        if sources:
                            update_tables.add(table_name)
            
            # Find reference tables: tables used as information sources in the script
            # This includes both direct table references and base tables that views depend on
            
            # Process views first to find their base tables
            for view_name in filtered_views:
                if not self.is_volatile(view_name):
                    # For views, find their underlying base tables using lineage information
                    self._add_view_base_tables(view_name, reference_tables)
            
            # Process tables
            for table_name in filtered_tables:
                if not self.is_volatile(table_name):
                    # For tables, only add them as reference tables if they are actually referenced
                    # (not just being updated). Check if this table has target entries (is referenced by others)
                    if script_name in self.all_lineage_data:
                        lineage_data = self.all_lineage_data[script_name]
                        tables = lineage_data.get('tables', {})
                        if table_name in tables:
                            table_info = tables[table_name]
                            targets = table_info.get('target', [])
                            if targets:
                                reference_tables.add(table_name)
            
            result[script_name] = {
                "tables": sorted(sorted_tables),
                "views": sorted_views,  # Keep topological sort order
                "update_tables": sorted(update_tables),
                "reference_tables": sorted(reference_tables)
            }
        
        # Sort the result dictionary by script names alphabetically
        return dict(sorted(result.items()))


def main():
    """Main function to run the metadata analyzer."""
    import sys
    
    # Get lineage directory from command line argument or use default
    if len(sys.argv) > 1:
        lineage_dir = sys.argv[1]
    else:
        lineage_dir = "old_code/lineage"
    
    print(f"Using lineage directory: {lineage_dir}")
    
    # Create analyzer
    analyzer = MetadataAnalyzer(lineage_dir)
    
    # Load all lineage data
    analyzer.load_all_lineage_data()
    
    # Generate metadata
    metadata = analyzer.generate_metadata()
    
    # Write output
    output_file = "script_metadata.json"
    with open(output_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Metadata written to {output_file}")
    print(f"Processed {len(metadata)} scripts")
    
    # Print summary
    for script_name, info in metadata.items():
        print(f"\n{script_name}:")
        print(f"  Tables: {len(info['tables'])}")
        print(f"  Views: {len(info['views'])}")
        print(f"  Update Tables: {len(info['update_tables'])}")
        print(f"  Reference Tables: {len(info['reference_tables'])}")


if __name__ == "__main__":
    main()
