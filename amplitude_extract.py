import os
from dotenv import load_dotenv
import requests 

# Load .env file
load_dotenv() 

# Define variables
start_time = '20251104T00'
end_time = '20251105T00'
api_key = os.getenv("AMP_API_KEY")
api_secret = os.getenv("AMP_SECRET_KEY")

url = 'https://analytics.eu.amplitude.com/api/2/events'
params = {
    'start': start_time,
    'end': end_time
}

# Make the GET request with basic authentication
response = requests.get(url, params=params, auth=(api_key, api_secret))

# # Check the response status
# if response.status_code == 200:
#     # The request was successful
#     data = response.content 
#     print('Data retrieved successfully.')
#     # JSON data files saved to a zip folder 'data.zip'
#     with open('data/data.zip', 'wb') as file:
#         file.write(data)
# else:
#     # The request failed; print the error
#     print(f'Error {response.status_code}: {response.text}')

data = response.content 
print(data)