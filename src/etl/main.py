from extract.planes import extract_planes_data


def main() :
    """
    Main function to run the ETL process for planes data extraction.
    """
    try: 
        planes_data = extract_planes_data()
    except Exception as e :
        print(f"Error during data extraction: {e}")
        return
    print("Extracted Planes Data:")
    print(planes_data)


if __name__ == "__main__" :
    main()