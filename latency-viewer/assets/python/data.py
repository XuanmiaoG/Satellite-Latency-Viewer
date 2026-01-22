#!/home/oper/py39env/bin/python
import cgi
import cgitb
import json
import sys
import os
import logging
import pandas as pd
import traceback

# Enable detailed CGI error reporting
cgitb.enable()

# Set up logging - use absolute path to ensure writability
# LOG_DIR = "./"  # Using /tmp which should be writable
# os.makedirs(LOG_DIR, exist_ok=True)
# LOG_FILE = os.path.join(LOG_DIR, "latency_viewer.log")

# logging.basicConfig(
#     level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     filename=LOG_FILE,
#     filemode='a'
# )
# logger = logging.getLogger()

# Log script startup and environment information
# logger.info("=" * 80)
# logger.info("Data.py script starting")
# logger.info(f"Current working directory: {os.getcwd()}")
# logger.info(f"Script path: {os.path.abspath(__file__)}")
# logger.info(f"Python version: {sys.version}")
# logger.info(f"User running script: {os.getenv('USER') or 'Unknown'}")

# Import functions from our shared module
# Define fallback functions in case import fails
def fallback_get_canonical_id(satellite_id):
    """Fallback function if import fails - returns the input as-is"""
    # logger.warning(f"Using fallback get_canonical_id for {satellite_id}")
    return satellite_id

def fallback_get_all_variants(canonical_id):
    """Fallback function if import fails - returns the canonical ID in a list"""
    # logger.warning(f"Using fallback get_all_variants for {canonical_id}")
    return [canonical_id]

def fallback_run_sat_latency_query(start_time, end_time, filters=None):
    """Fallback function if import fails - returns empty list"""
    # logger.error("Using fallback run_sat_latency_query - no data will be returned")
    return []

# Set default functions to fallbacks
get_canonical_id = fallback_get_canonical_id
get_all_variants = fallback_get_all_variants
run_sat_latency_query = fallback_run_sat_latency_query

# Try to import the real functions
# try:
# Add possible module locations to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Also try parent directory
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# logger.info(f"Looking for sat_db_functions in: {current_dir} and {parent_dir}")

# List directory contents to verify module existence
dir_contents = os.listdir(current_dir)
# logger.info(f"Directory contents: {dir_contents}")

# Try to import the module
import sat_db_functions

# If successful, override the fallback functions
get_canonical_id = sat_db_functions.get_canonical_id
get_all_variants = sat_db_functions.get_all_variants
run_sat_latency_query = sat_db_functions.run_sat_latency_query

    # logger.info("Successfully imported from sat_db_functions")
# except ImportError as e:
#     logger.error(f"Import error: {str(e)}")
#     logger.error(traceback.format_exc())
#     logger.error("Will use fallback functions that provide limited functionality")
# except Exception as e:
#     logger.error(f"Unexpected error during import: {str(e)}")
#     logger.error(traceback.format_exc())
#     logger.error("Will use fallback functions that provide limited functionality")

def data_endpoint():
    """
    API endpoint to query satellite latency data directly from the database
    """
    try:
        # Get query parameters
        form = cgi.FieldStorage()
        start_date = form.getvalue("start_date")
        end_date = form.getvalue("end_date")
        start_hour = form.getvalue("start_hour", "00:00")
        end_hour = form.getvalue("end_hour", "23:59")
        
        # logger.info(f"Query parameters: start_date={start_date}, end_date={end_date}, start_hour={start_hour}, end_hour={end_hour}")
        
        # Convert date and time to ISO format
        start_datetime = f"{start_date}T{start_hour}:00"
        end_datetime = f"{end_date}T{end_hour}:59"
        
        # Get filter parameters
        satellite_id = form.getvalue("satellite_id")
        coverage = form.getvalue("coverage")
        instrument = form.getvalue("instrument")
        
        # logger.info(f"Filter parameters: satellite_id={satellite_id}, coverage={coverage}, instrument={instrument}")
        
        # Prepare filters
        filters = {}
        if satellite_id:
            # Get the canonical form
            # logger.info(f"Getting canonical form for: {satellite_id}")
            canonical_id = get_canonical_id(satellite_id)
            # logger.info(f"Canonical ID: {canonical_id}")
            
            # Get all variants of this canonical ID
            all_variants = get_all_variants(canonical_id)
            # logger.info(f"All variants: {all_variants}")
            
            # Use all variants in the filter
            filters["satellite-id"] = all_variants
            
            # logger.info(f"Expanded satellite ID {satellite_id} to variants: {all_variants}")
        
        if coverage:
            filters["coverage"] = coverage
        
        if instrument:
            filters["instrument"] = instrument
        
        # logger.info(f"Data request - Period: {start_datetime} to {end_datetime}, Filters: {filters}")
        
        # Query the database
        # logger.info("About to call run_sat_latency_query...")
        try:
            data = run_sat_latency_query(start_datetime, end_datetime, filters)
            # logger.info(f"Query returned: {len(data) if data else 0} records")
        except Exception as query_error:
            # logger.error(f"Error in run_sat_latency_query: {str(query_error)}")
            # logger.error(traceback.format_exc())
            return {"message": f"Database query error: {str(query_error)}", "data": []}, 500
        
        if not data:
            # logger.info("Query returned no data")
            return {"message": "No data available for the selected period.", "data": []}
        
        # Convert to DataFrame for easier processing
        # logger.info("Converting to DataFrame...")
        try:
            df = pd.DataFrame(data)
            # logger.info(f"DataFrame created with shape: {df.shape}")
        except Exception as df_error:
            # logger.error(f"Error creating DataFrame: {str(df_error)}")
            # logger.error(traceback.format_exc())
            return {"message": f"Error creating DataFrame: {str(df_error)}", "data": []}, 500
        
        # Clean and process data
        try:
            # logger.info("Processing DataFrame...")
            
            # Normalize column names (case-insensitive matching)
            df.columns = [col.lower() for col in df.columns]
            # logger.info(f"Columns after normalization: {list(df.columns)}")
            
            # Clean latency data
            # logger.info("Cleaning latency data...")
            df['latency'] = pd.to_numeric(df['latency'], errors='coerce')
            df = df.dropna(subset=['latency'])
            # logger.info(f"DataFrame shape after cleaning: {df.shape}")
            
            # Add missing columns with 'Not Available' default
            default_columns = ['ingest_source', 'coverage', 'instrument', 'band', 'section', 'satellite_id']
            for col in default_columns:
                if col not in df.columns:
                    # logger.warning(f"Column '{col}' not found. Adding with default value.")
                    df[col] = 'Not Available'
            
            # Fill NaN values with "Not Available"
            for col in default_columns:
                df[col] = df[col].fillna('Not Available')
            
            # Add canonical_satellite_id column
            if 'satellite_id' in df.columns:
                # logger.info("Adding canonical_satellite_id column...")
                df['canonical_satellite_id'] = df['satellite_id'].apply(get_canonical_id)
            
            # Convert timestamps to string for JSON serialization
            if 'start_time' in df.columns:
                # logger.info("Converting timestamps...")
                # Most flexible approach:
                df['start_time'] = pd.to_datetime(df['start_time'], format='mixed', errors='coerce').astype(str)
            
            # Convert to records and handle NaN values
            # logger.info("Converting to records...")
            result = df.replace({pd.NA: "Not Available", pd.NaT: "Not Available"}).to_dict(orient="records")
            # logger.info(f"Created {len(result)} result records")
            
            return {
                "data": result,
                "metadata": {
                    "instruments": df['instrument'].unique().tolist(),
                    "coverages": df['coverage'].unique().tolist(),
                    "total_records": len(result)
                }
            }
            
        except Exception as e:
            # logger.error(f"Error during data processing: {str(e)}")
            # logger.error(traceback.format_exc())
            return {"message": f"Data processing error: {str(e)}", "data": []}, 500
            
    except Exception as e:
        # logger.error(f"Error processing data request: {str(e)}")
        # logger.error(traceback.format_exc())
        return {"message": f"Internal Server Error: {str(e)}", "data": []}, 500

# Main entry point for CGI
if __name__ == "__main__":
    # Set content-type header for JSON response
    print("Content-Type: application/json")
    print()  # Empty line after headers
    
    try:
        # Get the result from our endpoint function
        # logger.info("Calling data_endpoint function...")
        result = data_endpoint()
        
        # Handle tuple returns (for error responses)
        if isinstance(result, tuple):
            response_data, status_code = result
            # logger.warning(f"Returning error with status code {status_code}: {response_data}")
        else:
            response_data, status_code = result, 200
        
        # Print JSON response
        # logger.info(f"Returning response with status code {status_code} and {len(response_data.get('data', []))} records")
        print(json.dumps(response_data))
        
    except Exception as final_error:
        # logger.error(f"Final error in main block: {str(final_error)}")
        # logger.error(traceback.format_exc())
        
        # Attempt to return a meaningful error
        error_response = {
            "error": "Critical error in script execution",
            "message": str(final_error),
            "data": []
        }
        print(json.dumps(error_response))