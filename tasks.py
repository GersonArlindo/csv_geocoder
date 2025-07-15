import os
import pandas as pd
import time
from celery import Celery
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import googlemaps

# --- Celery Configuration ---
# The first argument is the name of the current module.
# The 'broker' is the URL to our Redis server.
# The 'backend' is also Redis, used to store task results.
celery_app = Celery('tasks', broker='redis://82.180.162.252:6379/0', backend='redis://82.180.162.252:6379/0')

# --- Google Maps API Key (loaded from environment variable for security) ---
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY', '')

# Initialize the Google Maps client
if "TU_CLAVE" in GOOGLE_API_KEY or not GOOGLE_API_KEY:
    print("WORKER WARNING: Google API Key is not set. Fallback will not work.")
    gmaps = None
else:
    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

# --- THE GEOCODING LOGIC IS NOW A CELERY TASK ---
@celery_app.task
def geocode_csv_task(input_path, output_path):
    """
    A Celery task that reads a CSV, geocodes it, and saves the result.
    """
    print(f"Worker received task for file: {input_path}")
    try:
        # --- Geocoder 1: Nominatim ---
        nominatim_geolocator = Nominatim(user_agent=f"geocoder_worker_{time.time()}", timeout=10)
        nominatim_geocode = RateLimiter(nominatim_geolocator.geocode, min_delay_seconds=1)

        def get_coords_with_fallback(address):
            if not isinstance(address, str) or not address.strip():
                return pd.Series([None, None, "Address is empty"])
            
            # Try Nominatim
            try:
                location = nominatim_geocode(address)
                if location:
                    return pd.Series([location.latitude, location.longitude, "Nominatim"])
            except Exception:
                pass # Ignore Nominatim errors and proceed to Google

            # Fallback to Google
            if gmaps:
                try:
                    geocode_result = gmaps.geocode(address)
                    if geocode_result:
                        lat = geocode_result[0]['geometry']['location']['lat']
                        lng = geocode_result[0]['geometry']['location']['lng']
                        return pd.Series([lat, lng, "Google"])
                except Exception:
                    pass # Ignore Google errors as well
            
            return pd.Series([None, None, "Failed"])

        # Read the original CSV (delimiter detection happens in the Flask app)
        # For simplicity here, we assume the delimiter detection was handled before.
        # A more robust solution would pass the delimiter as an argument.
        try:
            df = pd.read_csv(input_path, sep=';')
        except Exception:
            df = pd.read_csv(input_path, sep=',')

        if 'FULL_ADDRESS' not in df.columns:
            raise ValueError("FULL_ADDRESS column not found.")

        df[['lat', 'lng', 'geocoding_source']] = df['FULL_ADDRESS'].apply(get_coords_with_fallback)
        
        # Save the processed file
        df.to_csv(output_path, index=False)
        print(f"Task completed. Processed file saved to: {output_path}")
        
        # Clean up the original uploaded file
        if os.path.exists(input_path):
            os.remove(input_path)

    except Exception as e:
        print(f"An error occurred in the worker task: {e}")
        # You could add more robust error handling here, like updating a task status to 'FAILED'
    
    return output_path
