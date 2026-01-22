# Satellite Latency Viewer

This tool generates relationship information between satellites for the latency project by creating a `satellite_relationships.json` file.

## Prerequisites

- Python 3.9 or higher
- Apache web server with CGI enabled
- Proper file permissions

## Installation

1. Ensure Python 3.9+ is installed on your system
2. Make sure all Python scripts have proper execution permissions:
   ```
   chmod +x generate_relationship.py
   chmod +x satellites.py
   chmod +x sat_db_functions.py
   chmod +x data.py
   ```
3. Verify that the shebang line in all Python scripts points to the correct Python interpreter:
   ```
   #!/home/oper/py39env/bin/python
   ```
4. Make sure install Max's sat_latency python library using this and install the requirements.txt
   ```
   pip install -r requirements.txt
   pip install sds-sat-latency --index-url https://gitlab.ssec.wisc.edu/api/v4/projects/2693/packages/pypi/simple

   ```
   I add a pre-build environment py39env.zip in the gitlab repo https://gitlab.ssec.wisc.edu/ygao/latency_py39env#
   git clone https://gitlab.ssec.wisc.edu/ygao/latency_py39env.git
## Usage

### Generating Satellite Relationships

To generate the `satellite_relationships.json` file, run:

```bash
python3 generate_relationship.py
```

This will create or update the `satellite_relationships.json` file with the latest relationship data between satellites.

### Common Issues and Troubleshooting

#### Satellite ID Mapping

Both `satellites.py` and `sat_db_functions.py` contain a hardcoded mapping dictionary to normalize satellite IDs:

```python
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
```

This mapping is used to normalize different satellite ID formats to their canonical forms.

#### Web Server Configuration

Make sure your Apache configuration allows CGI script execution. The included `.htaccess` file contains the necessary settings:

```
Options +ExecCGI -Indexes
AddHandler cgi-script .py
```

#### "Internal Server Error" Troubleshooting

If you're getting "Internal Server Error" in the web interface:

1. Check file permissions:
   ```bash
   chmod 755 data.py sat_db_functions.py
   ```

2. Check the latency viewer log file permissions:
   ```bash
   chmod 644 latency_viewer.log
   ```
   Make sure the directory it's in is writable by the web server.

3. Verify the Python path in the shebang line of all scripts:
   ```
   #!/home/oper/py39env/bin/python
   ```



## File Structure

- `generate_relationship.py` - Main script to generate satellite relationships
- `satellites.py` - Contains satellite configuration and helper functions
- `sat_db_functions.py` - Database functions for satellite data
- `data.py` - Data processing utilities
- `satellite_relationships.json` - Output file containing relationship information

## Notes

- The system requires Python 3.9 or higher due to specific feature dependencies
- All satellite IDs are normalized using the `SATELLITE_ID_MAPPINGS` dictionary
- Web access is configured through Apache CGI with proper CORS headers for API access

## Author

Maintained and authored by Yang Gao.
