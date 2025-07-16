import os
import pandas as pd
import time
import json
import redis
from celery import Celery
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import googlemaps
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from urllib.parse import quote

# --- Celery Configuration ---
celery_app = Celery('tasks', broker='redis://82.180.162.252:6379/0', backend='redis://82.180.162.252:6379/0')

# --- Google Maps API Key (fallback only) ---
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY', '')

# Initialize Google Maps client (fallback only)
if "TU_CLAVE" in GOOGLE_API_KEY or not GOOGLE_API_KEY:
    print("WORKER INFO: Google API Key not set. Using Nominatim only.")
    gmaps = None
else:
    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)
    print("WORKER INFO: Google Maps available as fallback.")

# Redis client para cache
try:
    redis_client = redis.Redis(host='82.180.162.252', port=6379, db=1, decode_responses=True)
    redis_client.ping()
    print("WORKER INFO: Redis cache connected successfully")
except Exception as e:
    print(f"WORKER WARNING: Redis cache connection failed: {e}")
    redis_client = None

# --- Cache Functions ---
def get_cached_coords(address):
    """Busca coordenadas en cache"""
    if not redis_client:
        return None
    try:
        cache_key = f"geocode:{address.lower().strip()}"
        cached = redis_client.get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception as e:
        print(f"Cache read error: {e}")
    return None

def cache_coords(address, coords):
    """Guarda coordenadas en cache por 30 días"""
    if not redis_client:
        return
    try:
        cache_key = f"geocode:{address.lower().strip()}"
        redis_client.setex(cache_key, 86400*30, json.dumps(coords))  # 30 días
    except Exception as e:
        print(f"Cache write error: {e}")

# --- Optimized Address Preprocessing ---
def preprocess_address(address):
    """Limpia y normaliza la dirección para mejor geocodificación"""
    if not isinstance(address, str):
        return ""
    
    # Limpieza básica
    address = address.strip()
    
    # Reemplazar abreviaciones comunes que mejoran resultados en Nominatim
    replacements = {
        ' st ': ' street ',
        ' st.': ' street',
        ' ave ': ' avenue ',
        ' ave.': ' avenue',
        ' blvd ': ' boulevard ',
        ' blvd.': ' boulevard',
        ' rd ': ' road ',
        ' rd.': ' road',
        ' dr ': ' drive ',
        ' dr.': ' drive',
        ' ln ': ' lane ',
        ' ln.': ' lane',
        ' ct ': ' court ',
        ' ct.': ' court',
        ' calle ': ' ',  # Para direcciones en español
        ' avenida ': ' avenue ',
        ' colonia ': ' ',
        ' col ': ' ',
        ' #': ' ',
        '  ': ' '  # Espacios dobles
    }
    
    address_lower = address.lower()
    for old, new in replacements.items():
        address_lower = address_lower.replace(old, new)
    
    return address_lower.strip()

# --- Thread-local Nominatim instances ---
def get_nominatim_geolocator():
    """Obtiene un geolocator Nominatim thread-local"""
    thread_local = threading.current_thread()
    
    if not hasattr(thread_local, 'nominatim_geolocator'):
        thread_local.nominatim_geolocator = Nominatim(
            user_agent=f"geocoder_worker_{int(time.time())}_{thread_local.ident}",
            timeout=8,  # Timeout más generoso
            domain='nominatim.openstreetmap.org',
            scheme='https'
        )
        print(f"Created new Nominatim geolocator for thread {thread_local.ident}")
    
    return thread_local.nominatim_geolocator

# --- Core Geocoding Function ---
def geocode_single_address(address):
    """
    Geocodifica una dirección priorizando Nominatim (gratis) 
    con Google Maps como fallback solo si es necesario
    """
    if not isinstance(address, str) or not address.strip():
        return [None, None, "Empty address"]
    
    original_address = address.strip()
    
    # Verificar cache primero
    cached = get_cached_coords(original_address)
    if cached:
        return cached
    
    # Preprocesar dirección para mejor resultado
    processed_address = preprocess_address(original_address)
    
    result = [None, None, "Failed"]
    
    # PRIORIDAD 1: Nominatim (OpenStreetMap) - GRATIS
    try:
        geolocator = get_nominatim_geolocator()
        location = geolocator.geocode(processed_address, exactly_one=True, limit=1)
        
        if location:
            result = [location.latitude, location.longitude, "Nominatim"]
            print(f"✓ Nominatim: {original_address[:50]}...")
        else:
            print(f"✗ Nominatim failed: {original_address[:50]}...")
            
    except Exception as e:
        print(f"✗ Nominatim error for '{original_address[:50]}...': {e}")
    
    # FALLBACK: Google Maps (si Nominatim falla y está disponible)
    if result[2] == "Failed" and gmaps:
        try:
            geocode_result = gmaps.geocode(original_address)
            if geocode_result:
                lat = geocode_result[0]['geometry']['location']['lat']
                lng = geocode_result[0]['geometry']['location']['lng']
                result = [lat, lng, "Google"]
                print(f"✓ Google fallback: {original_address[:50]}...")
            else:
                print(f"✗ Google fallback failed: {original_address[:50]}...")
                
        except Exception as e:
            print(f"✗ Google error for '{original_address[:50]}...': {e}")
    
    # Guardar en cache
    cache_coords(original_address, result)
    
    return result

# --- Batch Processing Function ---
def geocode_batch(addresses_batch, batch_index):
    """
    Geocodifica un lote de direcciones con rate limiting inteligente
    """
    results = []
    batch_size = len(addresses_batch)
    
    print(f"Processing batchs {batch_index} with {batch_size} addresses...")
    
    for i, address in enumerate(addresses_batch):
        result = geocode_single_address(address)
        results.append(result)
        
        # Rate limiting solo para Nominatim (para ser respetuosos con el servicio gratuito)
        if result[2] == "Nominatim":
            time.sleep(1)  # 1 segundos entre requests de Nominatim
        elif result[2] == "Google":
            time.sleep(0.1)  # Mínimo delay para Google (es de pago)
            
        # Progress dentro del batch
        if (i + 1) % 5 == 0:
            print(f"  Batch {batch_index} progress: {i + 1}/{batch_size}")
    
    print(f"✓ Batch {batch_index} completed!")
    return results

# --- Main Celery Task ---
@celery_app.task
def geocode_csv_task(input_path, output_path):
    """
    Celery task optimizado para geocodificación masiva de CSV
    Prioriza Nominatim (gratis) con Google Maps como fallback
    """
    task_id = geocode_csv_task.request.id
    print(f"=== TASK {task_id} STARTED ===")
    print(f"Input: {input_path}")
    print(f"Output: {output_path}")
    
    start_time = time.time()
    
    try:
        # === PASO 1: Leer CSV ===
        print("\n🔍 STEP 1: Reading CSV file...")
        try:
            df = pd.read_csv(input_path, sep=';')
            print(f"   ✓ CSV read with semicolon separator")
        except Exception:
            try:
                df = pd.read_csv(input_path, sep=',')
                print(f"   ✓ CSV read with comma separator")
            except Exception as e:
                raise ValueError(f"Could not read CSV file: {e}")
        
        total_rows = len(df)
        print(f"   ✓ Found {total_rows} rows")
        
        if 'FULL_ADDRESS' not in df.columns:
            available_cols = ', '.join(df.columns)
            # Este error será capturado por Celery y hará que el estado de la tarea sea 'FAILURE'.
            raise ValueError(f"Required column 'FULL_ADDRESS' not found. Available columns are: {available_cols}")

        # === PASO 2: Preparar datos ===
        print(f"\n📋 STEP 2: Preparing data...")
        addresses = df['FULL_ADDRESS'].fillna('').astype(str).tolist()
        
        # Filtrar direcciones vacías
        non_empty_addresses = [addr for addr in addresses if addr.strip()]
        empty_count = len(addresses) - len(non_empty_addresses)
        
        if empty_count > 0:
            print(f"   ⚠️  Found {empty_count} empty addresses")
        
        # === PASO 3: Verificar cache ===
        print(f"\n💾 STEP 3: Checking cache...")
        cached_count = 0
        if redis_client:
            for addr in non_empty_addresses:
                if get_cached_coords(addr):
                    cached_count += 1
            print(f"   ✓ Found {cached_count} cached addresses")
        
        # === PASO 4: Configurar procesamiento paralelo ===
        print(f"\n⚙️  STEP 4: Setting up parallel processing...")
        
        # Tamaño de batch optimizado: más pequeño para mejor rate limiting
        batch_size = 8  # Reducido para mejor control de rate limiting
        max_workers = 2  # Solo 2 workers para ser respetuosos con Nominatim
        
        batches = [addresses[i:i+batch_size] for i in range(0, len(addresses), batch_size)]
        num_batches = len(batches)
        
        print(f"   ✓ Created {num_batches} batches of ~{batch_size} addresses each")
        print(f"   ✓ Using {max_workers} parallel workers")
        
        # === PASO 5: Procesamiento paralelo ===
        print(f"\n🚀 STEP 5: Starting parallel geocoding...")
        print(f"   Priority: Nominatim (free) → Google Maps (fallback)")
        
        all_results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Enviar todos los lotes
            future_to_batch = {
                executor.submit(geocode_batch, batch, i): i 
                for i, batch in enumerate(batches)
            }
            
            # Procesar resultados conforme se completan
            for future in as_completed(future_to_batch):
                batch_index = future_to_batch[future]
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                    
                    completed = len(all_results)
                    percentage = (completed / total_rows) * 100
                    
                    print(f"   ✓ Batch {batch_index + 1}/{num_batches} completed")
                    print(f"   📊 Progress: {completed}/{total_rows} ({percentage:.1f}%)")
                    
                except Exception as e:
                    print(f"   ✗ Batch {batch_index} failed: {e}")
                    # Agregar resultados fallidos para mantener el orden
                    batch_size_actual = len(batches[batch_index])
                    failed_results = [[None, None, "Batch failed"]] * batch_size_actual
                    all_results.extend(failed_results)
        
        # === PASO 6: Asignar resultados ===
        print(f"\n📊 STEP 6: Assigning results to DataFrame...")
        
        # Asegurar que tenemos el número correcto de resultados
        if len(all_results) != total_rows:
            print(f"   ⚠️  Result count mismatch: {len(all_results)} vs {total_rows}")
            # Rellenar con valores fallidos si es necesario
            while len(all_results) < total_rows:
                all_results.append([None, None, "Missing result"])
        
        # Crear DataFrame con resultados
        results_df = pd.DataFrame(all_results, columns=['lat', 'lng', 'geocoding_source'])
        df = pd.concat([df, results_df], axis=1)
        
        # === PASO 7: Estadísticas ===
        print(f"\n📈 STEP 7: Generating statistics...")
        
        success_count = df[df['geocoding_source'].notna() & (df['geocoding_source'] != 'Failed')].shape[0]
        nominatim_count = df[df['geocoding_source'] == 'Nominatim'].shape[0]
        google_count = df[df['geocoding_source'] == 'Google'].shape[0]
        failed_count = total_rows - success_count
        
        processing_time = time.time() - start_time
        rate = total_rows / processing_time if processing_time > 0 else 0
        
        print(f"\n📊 FINAL STATISTICS:")
        print(f"   📝 Total addresses: {total_rows}")
        print(f"   ✅ Successfully geocoded: {success_count} ({success_count/total_rows*100:.1f}%)")
        print(f"   🆓 Nominatim (free): {nominatim_count} ({nominatim_count/total_rows*100:.1f}%)")
        print(f"   💰 Google Maps (paid): {google_count} ({google_count/total_rows*100:.1f}%)")
        print(f"   ❌ Failed: {failed_count} ({failed_count/total_rows*100:.1f}%)")
        print(f"   💾 Cache hits: {cached_count}")
        print(f"   ⏱️  Processing time: {processing_time:.2f} seconds")
        print(f"   📊 Rate: {rate:.2f} addresses/second")
        
        # === PASO 8: Guardar archivo ===
        print(f"\n💾 STEP 8: Saving processed file...")
        
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Guardar con encoding UTF-8
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"   ✓ File saved: {output_path}")
        
        # === PASO 9: Limpieza ===
        print(f"\n🧹 STEP 9: Cleanup...")
        if os.path.exists(input_path):
            os.remove(input_path)
            print(f"   ✓ Removed input file: {input_path}")
        
        print(f"\n🎉 TASK {task_id} COMPLETED SUCCESSFULLY!")
        print(f"   Processing time: {processing_time:.2f} seconds")
        print(f"   Success rate: {success_count/total_rows*100:.1f}%")
        print(f"   Cost efficiency: {nominatim_count/success_count*100:.1f}% free geocoding")
        
        return output_path
        
    except Exception as e:
        print(f"\n💥 FATAL ERROR in task {task_id}: {e}")
        import traceback
        traceback.print_exc()
        
        # Intentar limpiar archivo de entrada en caso de error
        try:
            if os.path.exists(input_path):
                os.remove(input_path)
        except:
            pass
            
        #raise e