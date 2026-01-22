#!/usr/bin/env python3
"""
End-to-end script to generate a satellite relationships JSON file.
This script:
1. Automatically pulls data for the previous 7 days using sat_latency_interface
2. Processes the data to extract relationship information
3. Generates the satellite_relationships.json file with consolidated IDs
4. Handles satellite ID variations (e.g., G16/g16, DMSP-17/dmsp17, etc.)

Example usage:
    python generate_satellite_relationships.py -o satellite_relationships.json
    python generate_satellite_relationships.py -d 2025-02-27 -n 7 -o satellite_relationships.json
"""

import os
import json
import argparse
import logging
import subprocess
import tempfile
from datetime import datetime, timedelta
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Path to satellite data directory
SATELLITE_DATA_DIR = "/data/sat_latency"

# Hard-coded mapping of satellite ID variations to canonical IDs
SATELLITE_ID_MAPPINGS = {
    # Format: 'variant': 'canonical'
    'G16': 'G16',
    'g16': 'G16',
    'G18': 'G18',
    'g18': 'G18',
    'G19': 'G19',
    'g19': 'G19',
    'DMSP-17': 'DMSP-17',
    'dmsp17': 'DMSP-17',
    'DMSP-18': 'DMSP-18',
    'dmsp18': 'DMSP-18',
    'DMSP-16': 'DMSP-16',
    'dmsp16': 'DMSP-16',
    'NOAA-19': 'NOAA-19',
    'n19': 'NOAA-19',
    'NOAA-20': 'NOAA-20',
    'n20': 'NOAA-20',
    'NOAA-21': 'NOAA-21',
    'n21': 'NOAA-21',
    'NOAA-18': 'NOAA-18',
    'n18': 'NOAA-18',
    'NOAA-15': 'NOAA-15',
    'n15': 'NOAA-15',
    # Add more mappings as needed
}

# Create reverse mapping (canonical to variants)
CANONICAL_TO_VARIANTS = {}
for variant, canonical in SATELLITE_ID_MAPPINGS.items():
    if canonical not in CANONICAL_TO_VARIANTS:
        CANONICAL_TO_VARIANTS[canonical] = []
    CANONICAL_TO_VARIANTS[canonical].append(variant)

def get_canonical_id(satellite_id):
    """Get canonical ID for a satellite ID variant"""
    return SATELLITE_ID_MAPPINGS.get(satellite_id, satellite_id)

def get_date_range(end_date_str=None, num_days=7):
    """
    Get date range for the previous num_days from the end_date.
    
    Args:
        end_date_str: End date string in YYYY-MM-DD format, or None for yesterday
        num_days: Number of days to go back
        
    Returns:
        list: List of date strings in YYYY-MM-DD format
    """
    # Use yesterday as the end date if not provided
    if not end_date_str:
        end_date = datetime.now() - timedelta(days=1)
    else:
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    # Generate list of dates
    date_list = []
    for i in range(num_days):
        date = end_date - timedelta(days=i)
        date_list.append(date.strftime('%Y-%m-%d'))
    
    return date_list

def run_sat_latency_query(start_date_str, end_date_str):
    """
    Run sat_latency_interface command to get data for the specified date range.
    
    Args:
        start_date_str: Start date string in YYYY-MM-DD format
        end_date_str: End date string in YYYY-MM-DD format
        
    Returns:
        list: Data returned by sat_latency_interface or None if error
    """
    # Build start and end time strings
    start_time = f"{start_date_str}T00:00:00"
    end_time = f"{end_date_str}T23:59:59"
    
    # Build the command
    base_cmd = "module load miniconda/3.6-base && source activate ~/.mdrexler_conda && sat_latency_interface"
    # Include all fields to ensure we get satellite_id
    cmd = f"{base_cmd} -d {SATELLITE_DATA_DIR} --from '{start_time}' --until '{end_time}' --output-type json"
    
    logger.info(f"Running command: {cmd}")
    
    try:
        # Create a temporary shell script to run the command
        with tempfile.NamedTemporaryFile(suffix='.sh', mode='w', delete=False) as temp_script:
            script_path = temp_script.name
            temp_script.write("#!/bin/bash\n")
            temp_script.write(cmd + "\n")
        
        # Make the script executable
        os.chmod(script_path, 0o755)
        
        # Run the script using sudo as the oper user
        sudo_cmd = ["sudo", "-u", "oper", "-i", script_path]
        
        logger.info(f"Executing: {' '.join(sudo_cmd)}")
        
        # Use PIPE for stdout and stderr
        process = subprocess.Popen(
            sudo_cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        
        # Get the output and error
        stdout, stderr = process.communicate()
        
        # Check if the command was successful
        if process.returncode != 0:
            logger.error(f"Command failed with exit code {process.returncode}: {stderr}")
            return None
        
        # Log the first part of the output
        if stdout:
            logger.info(f"Command output (first 200 chars): {stdout[:200]}...")
        else:
            logger.warning("Command returned empty output")
            return None
            
        # Parse the JSON output
        try:
            data = json.loads(stdout)
            logger.info(f"Successfully parsed JSON data: {len(data)} records found")
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON output: {e}")
            logger.error(f"Raw output (first 500 chars): {stdout[:500]}...")
            return None
            
    except Exception as e:
        logger.error(f"Error executing command: {str(e)}")
        return None
    finally:
        # Clean up temporary script
        if os.path.exists(script_path):
            os.remove(script_path)

def extract_relationships_from_data(data):
    """
    Extract relationship information from satellite data.
    
    Args:
        data: List of satellite data records
        
    Returns:
        dict: Structured relationship information
    """
    if not data:
        logger.error("No data to process")
        return None
        
    # Log sample of the data to debug column names
    if data and len(data) > 0:
        logger.info(f"Sample data record keys: {list(data[0].keys())}")
        logger.info(f"Sample data record: {json.dumps(data[0], indent=2)}")
    
    # Initialize collections
    all_satellites = set()
    all_coverages = set()
    all_instruments = set()
    relationships = defaultdict(lambda: {
        'coverages': set(),
        'instruments': set(),
        'coverage_instruments': defaultdict(set)
    })
    
    # Process each record
    for record in data:
        # Extract satellite ID (handle case variations in column name)
        satellite_id = record.get('satellite_id') or record.get('satellite_ID') or record.get('SATELLITE_ID')
        if not satellite_id:
            satellite_id = 'Not Available'
        
        # Extract coverage and instrument (handle case variations and null values)
        coverage = record.get('coverage') or record.get('COVERAGE')
        if not coverage or coverage is None:
            coverage = 'Not Available'
        
        instrument = record.get('instrument') or record.get('INSTRUMENT')
        if not instrument or instrument is None:
            instrument = 'Not Available'
        
        # Get canonical satellite ID
        canonical_id = get_canonical_id(satellite_id)
        
        # Add to collections
        all_satellites.add(satellite_id)  # Track original IDs first
        all_coverages.add(coverage)
        all_instruments.add(instrument)
        
        # Update relationships for the canonical ID
        relationships[canonical_id]['coverages'].add(coverage)
        relationships[canonical_id]['instruments'].add(instrument)
        relationships[canonical_id]['coverage_instruments'][coverage].add(instrument)
    
    # Convert sets to lists for JSON serialization
    result = {
        "satellites": [],  # We'll populate this with consolidated IDs
        "coverages": sorted(list(all_coverages)),
        "instruments": sorted(list(all_instruments)),
        "relationships": {}
    }
    
    # Group satellites by canonical ID
    satellite_groups = defaultdict(list)
    for sat_id in all_satellites:
        canonical_id = get_canonical_id(sat_id)
        satellite_groups[canonical_id].append(sat_id)
    
    # Use canonical IDs as the satellite list
    result["satellites"] = sorted(list(satellite_groups.keys()))
    
    # Add satellite variant information
    result["satellite_variants"] = {
        canonical: variants for canonical, variants in satellite_groups.items()
    }
    
    # Convert relationships to lists for JSON serialization
    for sat_id, rel in relationships.items():
        result["relationships"][sat_id] = {
            "coverages": sorted(list(rel["coverages"])),
            "instruments": sorted(list(rel["instruments"])),
            "coverage_instruments": {
                cov: sorted(list(instruments)) 
                for cov, instruments in rel["coverage_instruments"].items()
            }
        }
    
    return result

def generate_satellite_relationships(end_date_str=None, num_days=7):
    """
    Generate satellite relationships JSON by querying data for the specified date range.
    
    Args:
        end_date_str: End date string in YYYY-MM-DD format, or None for yesterday
        num_days: Number of days to go back
        
    Returns:
        dict: Structured relationship information
    """
    # Get the date range
    date_range = get_date_range(end_date_str, num_days)
    
    # Use the earliest and latest dates for the query
    start_date = date_range[-1]  # Last item (earliest date)
    end_date = date_range[0]    # First item (latest date)
    
    logger.info(f"Generating satellite relationships for date range: {start_date} to {end_date}")
    
    # Fetch data for the specified date range
    data = run_sat_latency_query(start_date, end_date)
    
    if not data:
        logger.error(f"Failed to get data for range {start_date} to {end_date}")
        return None
    
    # Extract relationships from data
    result = extract_relationships_from_data(data)
    
    if not result:
        logger.error("Failed to extract relationships from data")
        return None
    
    # Add date range information to the output
    result["date_range"] = {
        "start_date": start_date,
        "end_date": end_date,
        "days": num_days
    }
    
    return result

def save_relationships_json(relationships, output_file):
    """
    Save relationships JSON to file.
    
    Args:
        relationships: Structured relationship information
        output_file: Output file path
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with open(output_file, 'w') as f:
            json.dump(relationships, f, indent=2)
        
        logger.info(f"Successfully wrote relationships to {output_file}")
        logger.info(f"Found {len(relationships['satellites'])} satellites, "
                   f"{len(relationships['coverages'])} coverages, "
                   f"{len(relationships['instruments'])} instruments")
        return True
    except Exception as e:
        logger.error(f"Error writing output file: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Generate satellite relationships JSON.')
    parser.add_argument('-d', '--date', help='End date to query (YYYY-MM-DD). Defaults to yesterday')
    parser.add_argument('-n', '--days', type=int, default=7, help='Number of days to analyze (default: 7)')
    parser.add_argument('-o', '--output', default='satellite_relationships.json', help='Output JSON file path')
    
    args = parser.parse_args()
    
    # Generate relationships JSON
    result = generate_satellite_relationships(args.date, args.days)
    
    if not result:
        logger.error("Failed to generate relationships JSON")
        return
    
    # Save to file
    if not save_relationships_json(result, args.output):
        logger.error(f"Failed to save relationships to {args.output}")

if __name__ == "__main__":
    main()