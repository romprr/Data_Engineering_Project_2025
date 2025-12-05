# Contains utility functions for CSV file interactions
import csv
def read(file_path):
    """
    Read a CSV file and return its contents as a list of dictionaries.

    Args:
        file_path (str): The path to the CSV file.
    Returns:
        list: A list of dictionaries representing the CSV rows.
    """
    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]