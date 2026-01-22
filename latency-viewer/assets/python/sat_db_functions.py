#!/home/oper/py39env/bin/python
import os
import json
import logging
import pandas as pd
from datetime import datetime, timezone,timedelta
from sat_latency.interface import satellite_data_from_filters

# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        return super().default(obj)
                               
# Set up logging
logger = logging.getLogger()

# Path to database
SATELLITE_DATA_DIR = "/data/sat_latency"  # Path to your latency database
RELATIONSHIPS_FILE = "satellite_relationships.json"  # Path to your prebuilt relationships file

# Hard-coded mapping of satellite ID variations to canonical IDs
# This makes it easy for future developers to add or modify mappings
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
    'n21': 'NOAA-21'
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

def get_all_variants(canonical_id):
    """Get all variants for a canonical satellite ID"""
    return CANONICAL_TO_VARIANTS.get(canonical_id, [canonical_id])

def consolidate_satellite_data(original_data):
    """
    Consolidate satellite data using the hard-coded mapping
    """
    if not original_data:
        return None
        
    normalized_data = {
        "satellites": [],
        "coverages": original_data.get("coverages", []),
        "instruments": original_data.get("instruments", []),
        "relationships": {},
        "satellite_variants": {}  # Maps canonical IDs to their variants
    }
    
    # Group satellites by canonical ID
    satellite_groups = {}
    
    for sat_id in original_data.get("satellites", []):
        canonical_id = get_canonical_id(sat_id)
        
        if canonical_id not in satellite_groups:
            satellite_groups[canonical_id] = []
        
        satellite_groups[canonical_id].append(sat_id)
    
    # Use canonical IDs as the satellite list
    normalized_data["satellites"] = sorted(satellite_groups.keys())
    
    # Store variant mapping
    normalized_data["satellite_variants"] = satellite_groups
    
    # Merge relationships for each canonical ID
    for canonical_id, variants in satellite_groups.items():
        normalized_data["relationships"][canonical_id] = {
            "coverages": [],
            "instruments": [],
            "coverage_instruments": {}
        }
        
        # Merge relationship data from all variants
        for variant_id in variants:
            if variant_id not in original_data.get("relationships", {}):
                continue
                
            original_relationship = original_data["relationships"][variant_id]
            
            # Merge coverages
            for coverage in original_relationship.get("coverages", []):
                if coverage not in normalized_data["relationships"][canonical_id]["coverages"]:
                    normalized_data["relationships"][canonical_id]["coverages"].append(coverage)
            
            # Merge instruments
            for instrument in original_relationship.get("instruments", []):
                if instrument not in normalized_data["relationships"][canonical_id]["instruments"]:
                    normalized_data["relationships"][canonical_id]["instruments"].append(instrument)
            
            # Merge coverage_instruments
            for coverage, instruments in original_relationship.get("coverage_instruments", {}).items():
                if coverage not in normalized_data["relationships"][canonical_id]["coverage_instruments"]:
                    normalized_data["relationships"][canonical_id]["coverage_instruments"][coverage] = []
                
                for instrument in instruments:
                    if instrument not in normalized_data["relationships"][canonical_id]["coverage_instruments"][coverage]:
                        normalized_data["relationships"][canonical_id]["coverage_instruments"][coverage].append(instrument)
        
        # Sort arrays for consistent output
        normalized_data["relationships"][canonical_id]["coverages"].sort()
        normalized_data["relationships"][canonical_id]["instruments"].sort()
        
        for coverage in normalized_data["relationships"][canonical_id]["coverage_instruments"]:
            normalized_data["relationships"][canonical_id]["coverage_instruments"][coverage].sort()
    
    return normalized_data

def load_relationship_data():
    """
    Load prebuilt satellite relationship data from JSON file and consolidate duplicates.
    """
    try:
        if os.path.exists(RELATIONSHIPS_FILE):
            with open(RELATIONSHIPS_FILE, 'r') as f:
                relationships = json.load(f)
                
            # Consolidate satellite data to merge variants
            consolidated = consolidate_satellite_data(relationships)
            
            if consolidated:
                logger.info(f"Loaded and consolidated {len(consolidated['satellites'])} unique satellites from relationships")
                return consolidated
            else:
                logger.warning("Failed to consolidate relationships data")
                return relationships
        else:
            logger.warning(f"Relationships file not found: {RELATIONSHIPS_FILE}")
            return None
    except Exception as e:
        logger.error(f"Error loading relationship data: {str(e)}")
        return None

def run_sat_latency_query(start_time, end_time, filters=None):
    """
    Query the satellite latency database using sat_latency.interface package
    
    Args:
        start_time (str): Start time in ISO format (YYYY-MM-DDTHH:MM:SS)
        end_time (str): End time in ISO format (YYYY-MM-DDTHH:MM:SS)
        filters (dict): Optional filters for satellite_id, coverage, instrument, etc.
        
    Returns:
        list: List of latency records as dictionaries
    """
    try:
        logger.info(f"Querying satellite latency data from {start_time} to {end_time}")
        
        # Convert string ISO timestamps to datetime objects if they are strings
        if isinstance(start_time, str):
            start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        else:
            start_datetime = start_time
            
        if isinstance(end_time, str):
            end_datetime = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        else:
            end_datetime = end_time
        
        # Ensure timezone is set
        if start_datetime.tzinfo is None:
            start_datetime = start_datetime.replace(tzinfo=timezone.utc)
        if end_datetime.tzinfo is None:
            end_datetime = end_datetime.replace(tzinfo=timezone.utc)
            
        logger.info(f"Converted timestamps: {start_datetime} to {end_datetime}")
        
       # Initialize filter parameters for the sat_latency API
        satellite_ids = None
        coverage = None
        instrument = None

        # Process filters
        if filters:
            # Expand satellite IDs to include all variants
            if "satellite-id" in filters:
                satellite_id = filters["satellite-id"]
                        
                # Handle list or comma-separated list of satellites
                if isinstance(satellite_id, list):
                    expanded_ids = []
                    for sat_id in satellite_id:
                        canonical_id = get_canonical_id(sat_id)
                        expanded_ids.extend(get_all_variants(canonical_id))
                    # Remove duplicates
                    satellite_ids = list(set(expanded_ids))
                elif isinstance(satellite_id, str) and ',' in satellite_id:
                    satellite_ids_list = [s.strip() for s in satellite_id.split(',')]
                    expanded_ids = []
                    for sat_id in satellite_ids_list:
                        canonical_id = get_canonical_id(sat_id)
                        expanded_ids.extend(get_all_variants(canonical_id))
                    satellite_ids = list(set(expanded_ids))
                elif isinstance(satellite_id, str):
                    canonical_id = get_canonical_id(satellite_id)
                    satellite_ids = get_all_variants(canonical_id)
                
            # Get coverage filter
            if "coverage" in filters:
                coverage_value = filters["coverage"]
                # Convert coverage to a list if it's a string
                if isinstance(coverage_value, str):
                    if ',' in coverage_value:
                        coverage = [c.strip() for c in coverage_value.split(',')]
                    else:
                        coverage = [coverage_value]
                else:
                    coverage = coverage_value  # Already a list or None
                
            # Get instrument filter
            if "instrument" in filters:
                instrument_value = filters["instrument"]
                # Convert instrument to a list if it's a string
                if isinstance(instrument_value, str):
                    if ',' in instrument_value:
                        instrument = [i.strip() for i in instrument_value.split(',')]
                    else:
                        instrument = [instrument_value]
                else:
                    instrument = instrument_value  # Already a list or None

        # Log the query parameters
        logger.info(f"Query parameters: database={SATELLITE_DATA_DIR}, start_date={start_datetime}, end_date={end_datetime}")
        logger.info(f"Filters: satellite_ids={satellite_ids}, coverage={coverage}, instrument={instrument}")

        # Call the sat_latency.interface function
        data = satellite_data_from_filters(
            SATELLITE_DATA_DIR,
            start_date=start_datetime,
            end_date=end_datetime,
            satellite_ids=satellite_ids,
            coverages=coverage,
            instruments=instrument
        )
        # Convert result to a list of dictionaries for JSON serialization
        if data is not None:
            try:
                # Convert Polars DataFrame to list of dictionaries
                records = data.to_dicts()
                
                # Process datetime objects for JSON serialization
                processed_records = []
                for record in records:
                    processed_record = {}
                    for key, value in record.items():
                        if isinstance(value, (datetime, pd.Timestamp)):
                            processed_record[key] = value.isoformat()
                        else:
                            processed_record[key] = value
                    processed_records.append(processed_record)
                
                logger.info(f"Successfully converted data: {len(processed_records)} records found")
                return processed_records
                
            except Exception as e:
                logger.error(f"Error converting Polars DataFrame to dict: {str(e)}")
                # Fallback method if to_dicts() is not available
                try:
                    pandas_df = data.to_pandas()
                    
                    # Convert datetime columns to strings
                    for col in pandas_df.select_dtypes(include=['datetime64']).columns:
                        pandas_df[col] = pandas_df[col].astype(str)
                    
                    records = pandas_df.to_dict(orient='records')
                    logger.info(f"Successfully converted data via pandas: {len(records)} records found")
                    return records
                except Exception as e2:
                    logger.error(f"Error in pandas conversion fallback: {str(e2)}")
                    return []
        else:
            logger.warning("Query returned None")
            return []
    except Exception as e:
        logger.error(f"Error executing satellite latency query: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []