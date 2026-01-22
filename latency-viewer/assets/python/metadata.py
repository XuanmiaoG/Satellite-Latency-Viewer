#!/home/oper/py39env/bin/python
import cgi
import json
import os
import sys

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
    # Define the path to the relationships file
    relationships_file = os.path.join(script_dir, "satellite_relationships.json")
    
    # Check if file exists
    if not os.path.exists(relationships_file):
        print(json.dumps({
            "error": f"Relationships file not found: {relationships_file}",
            "satellites": [],
            "coverages": [],
            "instruments": [],
            "relationships": {}
        }))
        sys.exit(0)
    
    # Load the relationships data
    with open(relationships_file, 'r') as f:
        raw_relationships = json.load(f)
    
    # Create normalized data structure
    normalized_data = {
        "satellites": [],
        "coverages": raw_relationships.get("coverages", []),
        "instruments": raw_relationships.get("instruments", []),
        "relationships": {},
        "satellite_variants": {}
    }
    
    # Group satellites by canonical ID
    satellite_groups = {}
    for sat_id in raw_relationships.get("satellites", []):
        canonical_id = SATELLITE_ID_MAPPINGS.get(sat_id, sat_id)
        
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
            if variant_id not in raw_relationships.get("relationships", {}):
                continue
                
            original_relationship = raw_relationships["relationships"][variant_id]
            
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
    
    # Return the normalized data
    print(json.dumps(normalized_data))

except Exception as e:
    import traceback
    print(json.dumps({
        "error": str(e),
        "traceback": traceback.format_exc(),
        "satellites": [],
        "coverages": [],
        "instruments": [],
        "relationships": {}
    }))