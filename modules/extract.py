import os
import zipfile
import gzip
import shutil
import tempfile
import requests
from datetime import datetime

def extract_amplitude_data(start_date, end_date, api_key, secret_key, output_file='data.zip'):
    """
    Extract data from Amplitude's Export API for a given date range.
    
    Args:
        start_date (str): Start date in format 'YYYYMMDDTHH' (e.g., '20241101T00')
        end_date (str): End date in format 'YYYYMMDDTHH' (e.g., '20241101T23')
        api_key (str): Amplitude API key
        secret_key (str): Amplitude secret key
        output_file (str): Output filename for the downloaded data (default: 'data.zip')
    
    Returns:
        bool: True if successful, False otherwise
    """
    # API endpoint for EU residency server
    url = 'https://analytics.eu.amplitude.com/api/2/export'
    params = {
        'start': start_date,
        'end': end_date
    }
    
    try:
        # Make the GET request with basic authentication
        response = requests.get(url, params=params, auth=(api_key, secret_key))
        
        # Check the response status
        if response.status_code == 200:
            # The request was successful
            data = response.content
            print('Data retrieved successfully.')
            
            # Save data to file
            with open(output_file, 'wb') as file:
                file.write(data)
            print(f'Data saved to {output_file}')
            return True
        else:
            # The request failed
            print(f'Error {response.status_code}: {response.text}')
            return False
            
    except Exception as e:
        print(f'An error occurred: {str(e)}')
        return False


# Example usage:
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    from datetime import datetime, timedelta
    
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('AMP_API_KEY')
    secret_key = os.getenv('AMP_SECRET_KEY')
    
    # Get yesterday's date (or 3 days ago as in your original code)
    target_date = datetime.now() - timedelta(days=3)
    start_time = target_date.strftime('%Y%m%dT00')
    end_time = target_date.strftime('%Y%m%dT23')
    
    # Call the function
    success = extract_amplitude_data(
        start_date=start_time,
        end_date=end_time,
        api_key=api_key,
        secret_key=secret_key
    )

def extract_json_files(zip_file_path, output_dir="data"):
    """
    Extract and decompress JSON files from a zip archive containing gzipped files.
    
    Args:
        zip_file_path (str): Path to the zip file to extract
        output_dir (str): Directory to extract JSON files to (default: "data")
    
    Returns:
        int: Number of JSON files extracted
    
    Raises:
        FileNotFoundError: If zip file doesn't exist
        ValueError: If no numeric day folder is found
    """
    if not os.path.exists(zip_file_path):
        raise FileNotFoundError(f"Zip file not found: {zip_file_path}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    extracted_count = 0
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Extract main zip file
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)
        
        # Find the numeric day folder
        try:
            day_folder = next(f for f in os.listdir(temp_dir) if f.isdigit())
        except StopIteration:
            raise ValueError("No numeric day folder found in the zip file")
        
        day_path = os.path.join(temp_dir, day_folder)
        
        # Walk through and decompress all .gz files
        for root, _, files in os.walk(day_path):
            for file in files:
                if file.endswith('.gz'):
                    gz_path = os.path.join(root, file)
                    json_filename = file[:-3]  # Remove .gz extension
                    output_path = os.path.join(output_dir, json_filename)
                    
                    with gzip.open(gz_path, 'rb') as gz_file, open(output_path, 'wb') as out_file:
                        shutil.copyfileobj(gz_file, out_file)
                    
                    extracted_count += 1
    
    print(f"Extracted {extracted_count} JSON files to '{output_dir}' directory!")
    return extracted_count

