#!/usr/bin/env python3
import csv
import re
import sys
import subprocess
import uuid
import argparse

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Execute ClickHouse query with profiling and generate an enriched DOT graph.')
    parser.add_argument('sql_query', help='SQL query to execute and profile')
    parser.add_argument('--host', default='localhost', help='ClickHouse server host (default: localhost)')
    parser.add_argument('--secure', action='store_true', help='Use secure connection to ClickHouse')
    parser.add_argument('--password', default='', help='ClickHouse server password (default: empty)')
    
    return parser.parse_args()

def get_clickhouse_base_command(args):
    """
    Build the base ClickHouse command with connection parameters.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        List of base command parameters
    """
    cmd = ['clickhouse-client', '--host', args.host]
    
    if args.secure:
        cmd.append('--secure')
        
    if args.password:
        cmd.extend(['--password', args.password])
        
    return cmd

def get_dot_graph_from_query(query, args):
    """
    Execute ClickHouse explain command to get the DOT graph for a query.

    Args:
        query: SQL query text
        args: Parsed command line arguments

    Returns:
        DOT graph as a string
    """
    try:
        # Get base command with connection parameters
        clickhouse_cmd = get_clickhouse_base_command(args)

        # Add explain command
        clickhouse_cmd.extend([
            '-q',
            f"explain pipeline compact=0,graph=1 {query}"
        ])

        # Execute the command and capture output
        result = subprocess.run(clickhouse_cmd, capture_output=True, text=True, check=True)

        # Return the DOT graph from stdout
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error executing EXPLAIN command: {e}", file=sys.stderr)
        print(f"Error output: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)

def execute_query_with_profiling(query, query_id, args):
    """
    Execute the actual query with profiling enabled.

    Args:
        query: SQL query text
        query_id: Custom query ID to use
        args: Parsed command line arguments
    """
    try:
        # Get base command with connection parameters
        clickhouse_cmd = get_clickhouse_base_command(args)
        
        # Add profiling parameters and query
        clickhouse_cmd.extend([
            '--log_processors_profiles', '1',
            '--query_id', query_id,
            '--format', 'Null',
            '-q',
            query
        ])

        # Execute the command and capture output
        result = subprocess.run(clickhouse_cmd, capture_output=True, text=True, check=True)

        print(f"Query executed successfully with no output (using Null format)", file=sys.stderr)

    except subprocess.CalledProcessError as e:
        print(f"Error executing query: {e}", file=sys.stderr)
        print(f"Error output: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)

def get_profile_data(query_id, args):
    """
    Get profiling data for a specific query_id.

    Args:
        query_id: The query ID to retrieve profiling data for
        args: Parsed command line arguments

    Returns:
        CSV profiling data as a string
    """
    try:
        # Get base command with connection parameters
        clickhouse_cmd = get_clickhouse_base_command(args)
        
        # Query the system.processors_profile_log table for the specific query_id
        profile_query = f"""
        SELECT
            toString(step_uniq_id) AS "step_id",
            toString(processor_uniq_id) AS "processor_id",
            elapsed_us
        FROM system.processors_profile_log
        WHERE query_id = '{query_id}'
        FORMAT CSVWithNames
        """

        # Add query parameter
        clickhouse_cmd.extend([
            '-q',
            profile_query
        ])

        # Execute the command and capture output
        result = subprocess.run(clickhouse_cmd, capture_output=True, text=True, check=True)

        # Return the CSV data from stdout
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Error retrieving profile data: {e}", file=sys.stderr)
        print(f"Error output: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)

def read_csv_data(csv_content):
    """Read the CSV data and organize it by processor_id."""
    result = {}

    # Split content into lines
    lines = csv_content.strip().split('\n')

    # Make sure there's data to process
    if not lines or len(lines) < 2:  # Need at least header and one data row
        print("Warning: No profile data found", file=sys.stderr)
        return result

    # Get header indices
    header = lines[0].split(',')
    step_id_idx = header.index('"step_id"')
    processor_id_idx = header.index('"processor_id"')
    elapsed_us_idx = header.index('"elapsed_us"')

    # Process data rows
    for i in range(1, len(lines)):
        row = lines[i].split(',')
        step_id = row[step_id_idx].strip('"')
        processor_id = row[processor_id_idx].strip('"')
        elapsed_us = row[elapsed_us_idx]

        if processor_id not in result:
            result[processor_id] = {"step_id": step_id, "elapsed_us": elapsed_us}

    return result

def enrich_dot_graph(dot_content, csv_content):
    """
    Enrich the DOT graph with information from the CSV by modifying
    node labels in place and preserving all other parts of the graph.
    """
    csv_data = read_csv_data(csv_content)

    # Create a pattern to find node labels in the dot file
    # This pattern captures the entire node definition line
    node_pattern = r'(n\d+\[label=")([^"]+)(".*?\];)'

    def replace_label(match):
        node_prefix = match.group(1)  # n0[label="
        label = match.group(2)        # NumbersRange_0
        node_suffix = match.group(3)  # "];

        # Check if we have data for this node
        if label in csv_data:
            data = csv_data[label]
            step_id = data["step_id"]
            elapsed_us = int(data["elapsed_us"])
            elapsed_ms = elapsed_us / 1000  # Convert microseconds to milliseconds

            # Create new enriched label with milliseconds
            new_label = f"{label}\\nStep: {step_id}\\nElapsed: {elapsed_ms:.2f} ms"
            return f"{node_prefix}{new_label}{node_suffix}"

        # If no data, return unchanged
        return match.group(0)

    # Replace all node labels with enriched versions
    enriched_dot = re.sub(node_pattern, replace_label, dot_content)

    return enriched_dot

def main():
    # Parse command line arguments
    args = parse_arguments()

    # STEP 1: Get DOT graph from EXPLAIN (without profiling)
    print("Getting DOT graph from EXPLAIN command...", file=sys.stderr)
    dot_content = get_dot_graph_from_query(args.sql_query, args)

    # STEP 2: Generate a unique query_id for the actual query
    query_id = f"profile_{uuid.uuid4().hex[:16]}"
    print(f"Using query_id: {query_id} for profiled query", file=sys.stderr)

    # STEP 3: Execute the actual query with profiling enabled
    print("Executing query with profiling enabled...", file=sys.stderr)
    execute_query_with_profiling(args.sql_query, query_id, args)

    # STEP 4: Flush logs to ensure profiling data is written
    print("Flushing logs...", file=sys.stderr)
    try:
        # Get base command with connection parameters
        flush_cmd = get_clickhouse_base_command(args)
        flush_cmd.extend(['-q', 'SYSTEM FLUSH LOGS'])
        
        subprocess.run(flush_cmd, check=True, capture_output=True)
    except Exception as e:
        print(f"Warning: Could not flush logs: {e}", file=sys.stderr)

    # STEP 5: Fetch profile data from system.processors_profile_log
    print("Retrieving profiling data...", file=sys.stderr)
    csv_content = get_profile_data(query_id, args)

    # STEP 6: Process and print enriched DOT graph to stdout
    print("Enriching DOT graph with profiling data...", file=sys.stderr)
    enriched_dot = enrich_dot_graph(dot_content, csv_content)

    # Print the final result to stdout (not stderr)
    print(enriched_dot)

if __name__ == "__main__":
    main()
