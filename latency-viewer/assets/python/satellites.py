#!/home/oper/py39env/bin/python
import cgi
import json
import sys
import os

# Print headers
print("Content-Type: application/json")
print()  # Empty line after headers

# Get script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Satellite ID mappings
SATELLITE_ID_MAPPINGS = {
    'G16': 'G16', 'g16': 'G16',
    'G18': 'G18', 'g18': 'G18',
    'G19': 'G19', 'g19': 'G19',
    'DMSP-17': 'DMSP-17', 'dmsp17': 'DMSP-17',
    'DMSP-18': 'DMSP-18', 'dmsp18': 'DMSP-18',
    'DMSP-16': 'DMSP-16', 'dmsp16': 'DMSP-16',
    'NOAA-19': 'NOAA-19', 'n19': 'NOAA-19',
    'NOAA-20': 'NOAA-20', 'n20': 'NOAA-20',
    'NOAA-21': 'NOAA-21', 'n21': 'NOAA-21'
}

try:
    # Get query parameters
    form = cgi.FieldStorage()
    date_str = form.getvalue("date")
    
    # Define the path to the relationships file
    relationships_file = os.path.join(script_dir, "satellite_relationships.json")
    
    # Check if file exists
    if not os.path.exists(relationships_file):
        print(json.dumps({
            "error": f"Relationships file not found: {relationships_file}",
            "satellites": [],
            "baseDir": "/data/sat_latency"
        }))
        sys.exit(0)
    
    # Load the relationships data
    with open(relationships_file, 'r') as f:
        raw_relationships = json.load(f)
    
    # Group satellites by canonical ID
    satellite_groups = {}
    for sat_id in raw_relationships.get("satellites", []):
        canonical_id = SATELLITE_ID_MAPPINGS.get(sat_id, sat_id)
        
        if canonical_id not in satellite_groups:
            satellite_groups[canonical_id] = []
        
        satellite_groups[canonical_id].append(sat_id)
    
    # Create the normalized list of satellites
    satellites = []
    for canonical_id, variants in satellite_groups.items():
        # Create display name with variants
        display_name = canonical_id
        if len(variants) > 1:
            variant_str = ", ".join([v for v in variants if v != canonical_id])
            if variant_str:
                display_name = f"{canonical_id} ({variant_str})"
        
        satellites.append({
            "id": canonical_id,
            "displayName": display_name,
            "fileExists": True
        })
    
    # Sort the satellites by ID
    satellites.sort(key=lambda x: x["id"])
    
    # Return the response
    print(json.dumps({
        "satellites": satellites,
        "baseDir": "/data/sat_latency",
        "normalized": True  # Flag to indicate normalization was performed
    }))

except Exception as e:
    import traceback
    print(json.dumps({
        "error": str(e),
        "traceback": traceback.format_exc(),
        "satellites": [],
        "baseDir": "/data/sat_latency"
    }))