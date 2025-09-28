#!/usr/bin/env python3
"""
Script to consolidate script_metadata.json into a single consolidated structure.
Takes a file path as input and generates a consolidated JSON file with _consolidated suffix.

The consolidated structure contains:
- tables: All unique tables sorted alphabetically
- views: All views maintaining topological order across scripts
- reference_tables: All unique reference tables sorted alphabetically
- updated_tables: All unique updated tables sorted alphabetically
- reference_only_tables: Reference tables that are not updated tables
"""

import json
import os
import sys
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple


class ScriptMetadataConsolidator:
    def __init__(self, input_file_path: str):
        self.input_file_path = input_file_path
        self.script_metadata = {}
        self.dependency_graph = defaultdict(set)
        self.reverse_dependency_graph = defaultdict(set)
        
    def load_script_metadata(self):
        """Load the script metadata from the input file."""
        try:
            with open(self.input_file_path, 'r') as f:
                self.script_metadata = json.load(f)
            print(f"Loaded script metadata from {self.input_file_path}")
            print(f"Found {len(self.script_metadata)} scripts")
        except Exception as e:
            print(f"Error loading script metadata: {e}")
            sys.exit(1)
    
    def build_dependency_graph(self):
        """Build dependency graph from views across all scripts to maintain topological order."""
        print("Building dependency graph for views...")
        
        # Collect all views and their dependencies
        for script_name, script_info in self.script_metadata.items():
            views = script_info.get('views', [])
            
            # For each view, find its dependencies by looking at other views in the same script
            for i, view in enumerate(views):
                # Views that come before this view in the same script are dependencies
                for j in range(i):
                    dependency = views[j]
                    if dependency != view:
                        self.dependency_graph[dependency].add(view)
                        self.reverse_dependency_graph[view].add(dependency)
    
    def topological_sort_views(self, all_views: Set[str]) -> List[str]:
        """Perform topological sort on all views to maintain dependency order."""
        # Build a subgraph with only the given views
        subgraph = defaultdict(set)
        in_degree = defaultdict(int)
        
        for view in all_views:
            in_degree[view] = 0
            for dependency in self.reverse_dependency_graph[view]:
                if dependency in all_views:
                    subgraph[dependency].add(view)
                    in_degree[view] += 1
        
        # Topological sort using Kahn's algorithm
        # Sort the initial queue to ensure consistent ordering for views with no dependencies
        queue = deque(sorted([view for view in all_views if in_degree[view] == 0]))
        result = []
        processed_count = 0
        
        while queue:
            current = queue.popleft()
            result.append(current)
            processed_count += 1
            
            # Add neighbors in sorted order to maintain consistent ordering
            neighbors = sorted(subgraph[current])
            for neighbor in neighbors:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        # Check if we processed all views (detect cycles)
        if processed_count != len(all_views):
            remaining_views = [view for view in all_views if view not in result]
            print(f"WARNING: Topological sort incomplete! {len(remaining_views)} views could not be sorted due to circular dependencies:")
            for view in remaining_views:
                print(f"  - {view}")
            print("These views will be appended to the end of the sorted list in alphabetical order.")
            
            # Try to identify specific cycles for better debugging
            self._detect_and_report_cycles(remaining_views, subgraph)
            
            # Add remaining views in alphabetical order
            result.extend(sorted(remaining_views))
        
        return result
    
    def _detect_and_report_cycles(self, remaining_views: List[str], subgraph: Dict[str, Set[str]]):
        """Detect and report specific cycles in the dependency graph."""
        visited = set()
        rec_stack = set()
        cycles = []
        
        def dfs_cycle_detection(node, path):
            if node in rec_stack:
                # Found a cycle
                cycle_start = path.index(node)
                cycle = path[cycle_start:] + [node]
                cycles.append(cycle)
                return True
            if node in visited:
                return False
                
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in subgraph.get(node, []):
                if neighbor in remaining_views:  # Only check remaining views
                    dfs_cycle_detection(neighbor, path.copy())
            
            rec_stack.remove(node)
            return False
        
        for view in remaining_views:
            if view not in visited:
                dfs_cycle_detection(view, [])
        
        if cycles:
            print("Detected circular dependencies:")
            for i, cycle in enumerate(cycles[:3]):  # Show first 3 cycles
                cycle_str = " -> ".join(cycle)
                print(f"  Cycle {i+1}: {cycle_str}")
            if len(cycles) > 3:
                print(f"  ... and {len(cycles) - 3} more cycles")
    
    def consolidate_metadata(self) -> Dict:
        """Consolidate all script metadata into a single structure."""
        print("Consolidating metadata...")
        
        # Build dependency graph first
        self.build_dependency_graph()
        
        # Collect all unique items
        all_tables = set()
        all_views = set()
        all_reference_tables = set()
        all_updated_tables = set()
        
        for script_name, script_info in self.script_metadata.items():
            # Collect tables
            tables = script_info.get('tables', [])
            all_tables.update(tables)
            
            # Collect views
            views = script_info.get('views', [])
            all_views.update(views)
            
            # Collect reference tables
            reference_tables = script_info.get('reference_tables', [])
            all_reference_tables.update(reference_tables)
            
            # Collect updated tables
            updated_tables = script_info.get('update_tables', [])
            all_updated_tables.update(updated_tables)
        
        # Sort tables alphabetically
        sorted_tables = sorted(all_tables)
        
        # Sort reference tables alphabetically
        sorted_reference_tables = sorted(all_reference_tables)
        
        # Sort updated tables alphabetically
        sorted_updated_tables = sorted(all_updated_tables)
        
        # Sort views using topological sort to maintain dependency order
        sorted_views = self.topological_sort_views(all_views)
        
        # Calculate reference_only_tables (reference_tables - updated_tables)
        reference_only_tables = sorted(all_reference_tables - all_updated_tables)
        
        consolidated = {
            "tables": sorted_tables,
            "views": sorted_views,
            "reference_tables": sorted_reference_tables,
            "updated_tables": sorted_updated_tables,
            "reference_only_tables": reference_only_tables
        }
        
        return consolidated
    
    def save_consolidated_metadata(self, consolidated_data: Dict):
        """Save the consolidated metadata to a new file with _consolidated suffix."""
        # Generate output file path
        base_name = os.path.splitext(self.input_file_path)[0]
        output_file = f"{base_name}_consolidated.json"
        
        try:
            with open(output_file, 'w') as f:
                json.dump(consolidated_data, f, indent=2)
            print(f"Consolidated metadata saved to {output_file}")
        except Exception as e:
            print(f"Error saving consolidated metadata: {e}")
            sys.exit(1)
    
    def print_summary(self, consolidated_data: Dict):
        """Print a summary of the consolidated metadata."""
        print("\n=== Consolidated Metadata Summary ===")
        print(f"Total tables: {len(consolidated_data['tables'])}")
        print(f"Total views: {len(consolidated_data['views'])}")
        print(f"Total reference tables: {len(consolidated_data['reference_tables'])}")
        print(f"Total updated tables: {len(consolidated_data['updated_tables'])}")
        print(f"Reference-only tables: {len(consolidated_data['reference_only_tables'])}")
        


def main():
    """Main function to run the consolidator."""
    if len(sys.argv) != 2:
        print("Usage: python consolidate_script_metadata.py <input_file_path>")
        print("Example: python consolidate_script_metadata.py script_metadata.json")
        sys.exit(1)
    
    input_file_path = sys.argv[1]
    
    # Check if input file exists
    if not os.path.exists(input_file_path):
        print(f"Error: Input file '{input_file_path}' does not exist")
        sys.exit(1)
    
    # Create consolidator
    consolidator = ScriptMetadataConsolidator(input_file_path)
    
    # Load script metadata
    consolidator.load_script_metadata()
    
    # Consolidate metadata
    consolidated_data = consolidator.consolidate_metadata()
    
    # Save consolidated metadata
    consolidator.save_consolidated_metadata(consolidated_data)
    
    # Print summary
    consolidator.print_summary(consolidated_data)


if __name__ == "__main__":
    main()
