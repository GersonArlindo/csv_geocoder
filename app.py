import os
from flask import Flask, request, render_template, jsonify, url_for, send_from_directory
from werkzeug.utils import secure_filename
import uuid
from tasks import geocode_csv_task # <--- IMPORT OUR CELERY TASK
import pandas as pd

app = Flask(__name__)

# Configuration
UPLOAD_FOLDER = 'uploads'
PROCESSED_FOLDER = 'processed'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

# --- Web Routes ---
@app.route('/')
def index():
    return render_template('index.html') # We will update this HTML later

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if not file or not file.filename.endswith('.csv'):
        return jsonify({'error': 'Invalid file type. Please upload a .csv file.'}), 400
    
    # --- Validación Rápida de Encabezado ---
    # Guardamos el archivo temporalmente para inspeccionarlo
    temp_id = str(uuid.uuid4())
    temp_path = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_{temp_id}.csv")
    file.save(temp_path)

    try:
        # Leemos solo el encabezado para verificar la columna
        # Intentamos con ambos delimitadores
        try:
            headers = pd.read_csv(temp_path, sep=';', nrows=0).columns.tolist()
        except Exception:
            headers = pd.read_csv(temp_path, sep=',', nrows=0).columns.tolist()

        if 'FULL_ADDRESS' not in headers:
            # Si la columna no existe, devolvemos un error INMEDIATO
            os.remove(temp_path) # Borramos el archivo temporal
            available_cols = ', '.join(headers)
            error_msg = f"Required column 'FULL_ADDRESS' not found. Available columns: {available_cols}"
            return jsonify({'error': error_msg}), 400 # Código 400 indica un error del cliente

    except Exception as e:
        # Si hay algún error leyendo el encabezado, también es un error
        os.remove(temp_path)
        return jsonify({'error': f'Could not read CSV headers. Please check file format. Error: {e}'}), 400
    


    if file and file.filename.endswith('.csv'):
        # Create secure and unique filenames to avoid conflicts
        original_filename = secure_filename(file.filename)
        task_id = str(uuid.uuid4())
        input_filename = f"{task_id}_{original_filename}"
        output_filename = f"geocoded_{input_filename}"
        
        input_path = os.path.join(app.config['UPLOAD_FOLDER'], input_filename)
        output_path = os.path.join(app.config['PROCESSED_FOLDER'], output_filename)
        
        file.save(input_path)

        # --- Launch the background task ---
        # We call .delay() to run it in the background with Celery
        geocode_csv_task.delay(input_path, output_path)
        
        # --- Respond to the user IMMEDIATELY ---
        return jsonify({
            'message': 'File uploaded successfully! Processing has started in the background.',
            'task_id': task_id,
            'status_url': url_for('task_status', task_id=task_id),
            'result_url': url_for('download_file', filename=output_filename)
        })
    
    return jsonify({'error': 'Invalid file type'}), 400

@app.route('/status/<task_id>')
def task_status(task_id):
    # Obtiene el resultado de la tarea asíncrona desde el backend de Celery
    task = geocode_csv_task.AsyncResult(task_id)

    if task.state == 'PENDING':
        # La tarea aún no ha sido recogida por un worker o está en proceso
        response = {
            'status': 'PENDING',
            'message': 'Task is waiting in the queue or currently processing.'
        }
    elif task.state == 'SUCCESS':
        # La tarea se completó con éxito
        filename = os.path.basename(task.result) # El resultado de la tarea es la ruta del archivo
        response = {
            'status': 'SUCCESS',
            'result_url': url_for('download_file', filename=filename),
            'message': 'Task completed successfully!'
        }
    elif task.state == 'FAILURE':
        # La tarea falló
        # task.info contiene la excepción y el traceback
        error_message = str(task.info) # Convertimos la excepción a texto
        response = {
            'status': 'FAILURE',
            'message': f"Task failed: {error_message}"
        }
    else:
        # Otro estado (como 'STARTED', 'RETRY')
        response = {
            'status': 'PROGRESS',
            'message': f'Task is in progress with state: {task.state}'
        }
        
    return jsonify(response)

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['PROCESSED_FOLDER'], filename, as_attachment=True)

if __name__ == '__main__':
    # Set the Google API Key as an environment variable before running
    # On Windows (cmd): set GOOGLE_API_KEY=TU_CLAVE_REAL
    # On macOS/Linux: export GOOGLE_API_KEY=TU_CLAVE_REAL
    app.run(host='0.0.0.0', port=5000)
