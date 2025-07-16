import os
from flask import Flask, request, render_template, jsonify, url_for, send_from_directory
from werkzeug.utils import secure_filename
import uuid
from tasks import geocode_csv_task # <--- IMPORT OUR CELERY TASK

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
    
    return jsonify({'errors': 'Invalid file type'}), 400

@app.route('/status/<task_id>')
def task_status(task_id):
    # This is a simplified status check. It just checks if the output file exists.
    # A more robust solution would use Celery's result backend to check the actual task state.
    filename = None
    for f in os.listdir(app.config['PROCESSED_FOLDER']):
        if f.startswith(f"geocoded_{task_id}"):
            filename = f
            break
            
    if filename:
        return jsonify({'status': 'SUCCESS', 'result_url': url_for('download_file', filename=filename)})
    else:
        return jsonify({'status': 'PENDING'})

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['PROCESSED_FOLDER'], filename, as_attachment=True)

if __name__ == '__main__':
    # Set the Google API Key as an environment variable before running
    # On Windows (cmd): set GOOGLE_API_KEY=TU_CLAVE_REAL
    # On macOS/Linux: export GOOGLE_API_KEY=TU_CLAVE_REAL
    app.run(host='0.0.0.0', port=5000)
