#!/usr/bin/env python3

import re
import sys
import subprocess
import uuid
import argparse


def parse_arguments():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Execute ClickHouse query with profiling and generate an enriched DOT graph with heatmap coloring. Distributed queries are not really supported."
    )
    parser.add_argument("sql_query", help="SQL query to profile")
    parser.add_argument(
        "--host",
        default="localhost",
        help="ClickHouse server host (default: localhost)",
    )
    parser.add_argument(
        "--secure", action="store_true", help="Use secure connection to ClickHouse"
    )
    parser.add_argument(
        "--password", default="", help="ClickHouse server password (default: empty)"
    )
    parser.add_argument("--cluster", help="ClickHouse cluster name")

    return parser.parse_args()


def get_clickhouse_base_command(args):
    """
    Build the base ClickHouse command with connection parameters.

    Args:
        args: Parsed command line arguments

    Returns:
        List of base command parameters
    """
    cmd = ["clickhouse-client", "--host", args.host]

    if args.secure:
        cmd.append("--secure")

    if args.password:
        cmd.extend(["--password", args.password])

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
        clickhouse_cmd.extend(["-q", f"explain pipeline compact=0,graph=1 {query}"])

        # Execute the command and capture output
        result = subprocess.run(
            clickhouse_cmd, capture_output=True, text=True, check=True
        )

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
        clickhouse_cmd.extend(
            [
                "--log_processors_profiles",
                "1",
                "--query_id",
                query_id,
                "--format",
                "Null",
                "-q",
                query,
            ]
        )

        # Execute the command and capture output
        subprocess.run(clickhouse_cmd, capture_output=True, text=True, check=True)

        print(
            "Query executed successfully with no output (using Null format)",
            file=sys.stderr,
        )

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
        TSV profiling data as a string
    """
    try:
        # Get base command with connection parameters
        clickhouse_cmd = get_clickhouse_base_command(args)

        # Determine which table to query based on cluster argument
        if args.cluster:
            table_reference = (
                f"clusterAllReplicas('{args.cluster}', system.processors_profile_log)"
            )
        else:
            table_reference = "system.processors_profile_log"

        # Query the appropriate table for the specific query_id
        profile_query = f"""
        SELECT
            toString(step_uniq_id) AS "step_id",
            toString(processor_uniq_id) AS "processor_id",
            elapsed_us
        FROM {table_reference}
        WHERE query_id = '{query_id}'
        FORMAT TSVWithNames
        """

        # Add query parameter
        clickhouse_cmd.extend(["-q", profile_query])

        # Execute the command and capture output
        result = subprocess.run(
            clickhouse_cmd, capture_output=True, text=True, check=True
        )

        # Return the TSV data from stdout
        return result.stdout.strip()

    except subprocess.CalledProcessError as e:
        print(f"Error retrieving profile data: {e}", file=sys.stderr)
        print(f"Error output: {e.stderr}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


def read_tsv_data(tsv_content):
    """
    Read the TSV data and organize it by processor_id.
    Also calculate min and max elapsed time for heatmap coloring.
    """
    result = {}
    max_elapsed_us = 0
    min_elapsed_us = float("inf")

    # Split content into lines
    lines = tsv_content.strip().split("\n")

    # Make sure there's data to process
    if not lines or len(lines) < 2:  # Need at least header and one data row
        print("Warning: No profile data found", file=sys.stderr)
        return result, min_elapsed_us, max_elapsed_us

    # Get header indices - handle both quoted and unquoted headers
    header = lines[0].split("\t")

    # Try to find the column indices, handling different possible formats
    step_id_idx = -1
    processor_id_idx = -1
    elapsed_us_idx = -1

    for i, column in enumerate(header):
        # Remove quotes if present
        clean_column = column.replace('"', "")

        if clean_column == "step_id":
            step_id_idx = i
        elif clean_column == "processor_id":
            processor_id_idx = i
        elif clean_column == "elapsed_us":
            elapsed_us_idx = i

    # Check if we found all required columns
    if step_id_idx == -1 or processor_id_idx == -1 or elapsed_us_idx == -1:
        print(
            f"Warning: Could not find all required columns in TSV header: {header}",
            file=sys.stderr,
        )
        return result, min_elapsed_us, max_elapsed_us

    # Process data rows
    for i in range(1, len(lines)):
        row = lines[i].split("\t")

        # Skip rows that don't have enough columns
        if len(row) <= max(step_id_idx, processor_id_idx, elapsed_us_idx):
            print(
                f"Warning: Skipping row with insufficient columns: {row}",
                file=sys.stderr,
            )
            continue

        # Remove all quotes from the values
        step_id = row[step_id_idx].replace('"', "")
        processor_id = row[processor_id_idx].replace('"', "")
        elapsed_us = row[elapsed_us_idx].replace('"', "")

        # Convert elapsed_us to a numeric value
        try:
            elapsed_us_value = int(elapsed_us)

            # Update min and max values for scaling
            if elapsed_us_value > max_elapsed_us:
                max_elapsed_us = elapsed_us_value
            if elapsed_us_value < min_elapsed_us:
                min_elapsed_us = elapsed_us_value

            # Store the data
            if processor_id not in result:
                result[processor_id] = {
                    "step_id": step_id,
                    "elapsed_us": elapsed_us_value,
                }

        except ValueError:
            print(
                f"Warning: Could not convert elapsed_us to int: {elapsed_us}",
                file=sys.stderr,
            )
            if processor_id not in result:
                result[processor_id] = {"step_id": step_id, "elapsed_us": elapsed_us}

    # If no data was processed, set min to 0
    if min_elapsed_us == float("inf"):
        min_elapsed_us = 0

    return result, min_elapsed_us, max_elapsed_us


def get_heatmap_color(elapsed_us, min_elapsed_us, max_elapsed_us):
    """
    Generate a color ranging from white to red based on the elapsed time.

    Args:
        elapsed_us: The time in microseconds
        min_elapsed_us: Minimum time across all processors
        max_elapsed_us: Maximum time across all processors

    Returns:
        A hex color string (e.g., "#FFFFFF" for white, "#FF0000" for red)
    """
    # Prevent division by zero if all processors took the same time
    range_elapsed = max_elapsed_us - min_elapsed_us
    if range_elapsed == 0:
        normalized = 0.5  # Default to mid-range if all times are the same
    else:
        # Normalize the value between 0 and 1
        normalized = (elapsed_us - min_elapsed_us) / range_elapsed

    # Convert to a color from white (#FFFFFF) to red (#FF0000)
    # Green component goes from FF to 00
    # Blue component goes from FF to 00
    green_blue = int(255 * (1 - normalized))

    # Format as hex color
    color = f'"#FF{green_blue:02X}{green_blue:02X}"'

    return color


def enrich_dot_graph(dot_content, tsv_content):
    """
    Enrich the DOT graph with information from the TSV by modifying
    node labels in place and adding heatmap coloring based on elapsed time.
    """
    # Get profile data and min/max times for scaling the heatmap
    tsv_data, min_elapsed_us, max_elapsed_us = read_tsv_data(tsv_content)

    print(
        f"Time range for heatmap: {min_elapsed_us/1000:.2f} - {max_elapsed_us/1000:.2f} ms",
        file=sys.stderr,
    )

    # Create a pattern to find node definitions in the dot file
    # This pattern captures the entire node definition line more precisely
    # It captures: (1) node id and label start, (2) the processor name, (3) the rest until the semicolon
    node_pattern = r'(n\d+\s*\[\s*label\s*=\s*")(.*?)("\s*\]\s*;)'

    def replace_label(match):
        node_prefix = match.group(1)  # n0[label="
        label = match.group(2)  # The processor name (with complex chars)
        node_suffix = match.group(3)  # "];

        # Check if we have data for this node
        if label in tsv_data:
            data = tsv_data[label]
            step_id = data["step_id"]

            # Process elapsed time
            try:
                # If already converted to int in read_tsv_data
                if isinstance(data["elapsed_us"], int):
                    elapsed_us = data["elapsed_us"]
                else:
                    # Otherwise convert it now
                    elapsed_us = int(data["elapsed_us"])

                elapsed_ms = elapsed_us / 1000  # Convert microseconds to milliseconds

                # Get color based on elapsed time
                color = get_heatmap_color(elapsed_us, min_elapsed_us, max_elapsed_us)

                # Create new enriched label with milliseconds
                new_label = f"{label}\\nStep: {step_id}\\nElapsed: {elapsed_ms:.2f} ms"

                # Add color styling to the node
                # We need to carefully preserve the existing attributes while adding our color ones
                # Replace the ending quotes and add color attributes before the closing bracket
                # The original suffix is like "]; - we need to modify it without breaking the syntax

                # The original suffix is typically in the form: "];
                # We need to replace it entirely without adding an extra "];
                # Instead of trying to preserve existing attributes, we'll replace the entire suffix

                # Create the new suffix with color attributes
                node_suffix_with_color = f'", fillcolor={color}, style="filled"];'

                return f"{node_prefix}{new_label}{node_suffix_with_color}"

            except ValueError:
                # In case of conversion error, use the original elapsed_us value
                print(
                    f"Warning: Could not convert elapsed_us to int: {data['elapsed_us']}",
                    file=sys.stderr,
                )
                new_label = f"{label}\\nStep: {step_id}\\nElapsed: {data['elapsed_us']}"
                return f"{node_prefix}{new_label}{node_suffix}"

        # If no data, return unchanged
        return match.group(0)

    # Modify the DOT graph header to ensure it supports node colors
    # We need to be careful with the DOT syntax - add the attributes in the correct location
    # Different ClickHouse versions might output slightly different DOT formats, so handle both common patterns
    if "digraph {" in dot_content and not "bgcolor" in dot_content:
        dot_content = dot_content.replace(
            "digraph {", 'digraph {\n  graph [bgcolor="transparent"];'
        )
    elif "digraph G {" in dot_content and not "bgcolor" in dot_content:
        dot_content = dot_content.replace(
            "digraph G {", 'digraph G {\n  graph [bgcolor="transparent"];'
        )

    # Replace all node labels with enriched versions including colors
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

        # Determine flush logs command based on cluster parameter
        if args.cluster:
            flush_query = f"SYSTEM FLUSH LOGS ON CLUSTER '{args.cluster}'"
        else:
            flush_query = "SYSTEM FLUSH LOGS"

        flush_cmd.extend(["-q", flush_query])

        subprocess.run(flush_cmd, check=True, capture_output=True)
    except Exception as e:
        print(f"Warning: Could not flush logs: {e}", file=sys.stderr)

    # STEP 5: Fetch profile data from system.processors_profile_log
    print("Retrieving profiling data...", file=sys.stderr)
    tsv_content = get_profile_data(query_id, args)

    # STEP 6: Process and print enriched DOT graph to stdout
    print(
        "Enriching DOT graph with profiling data and generating heatmap...",
        file=sys.stderr,
    )
    enriched_dot = enrich_dot_graph(dot_content, tsv_content)

    # Print the final result to stdout (not stderr)
    print("\n" + enriched_dot)


if __name__ == "__main__":
    main()
