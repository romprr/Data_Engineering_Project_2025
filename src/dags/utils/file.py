# Contains utility functions for file interactions
def write(data, file_path) :
    """Write data to a file in JSON format"""
    import json
    with open(file_path, 'a') as f:
        f.write(json.dumps(data) + "\n")
    