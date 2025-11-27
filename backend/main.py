from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import os
from data_cleaning import DataCleaner
from supabase_processor import SupabaseProcessor

app = Flask(__name__)
CORS(app)

# Configuration - REPLACE WITH YOUR SUPABASE CREDENTIALS
SUPABASE_URL = "https://xnraltsvlgxvddumkmuc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhucmFsdHN2bGd4dmRkdW1rbXVjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjQyMDAyNzksImV4cCI6MjA3OTc3NjI3OX0.TGg69kE4VhxFaJlOmxW_VH6iQWg9rvwa_G-FQ7_PKYA"

# Initialize components
cleaner = DataCleaner()
processor = SupabaseProcessor(SUPABASE_URL, SUPABASE_KEY)

@app.route('/')
def home():
    return jsonify({"message": "Airline Data Warehouse API", "status": "running"})

@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        upload_folder = 'uploads'
        os.makedirs(upload_folder, exist_ok=True)
        file_path = os.path.join(upload_folder, file.filename)
        file.save(file_path)
        
        return jsonify({
            'message': f'File uploaded successfully: {file.filename}',
            'file_path': file_path
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/process', methods=['POST'])
def process_data():
    try:
        data = request.json
        file_path = data.get('file_path')
        
        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 400
        
        filename = os.path.basename(file_path)
        df = pd.read_csv(file_path)
        
        processed_data = None
        dirty_rows = []
        
        if 'airport' in filename.lower():
            clean_df, dirty_rows = cleaner.clean_airports_data(df)
            processor.insert_airports(clean_df)
            processed_data = clean_df
            
        elif 'airline' in filename.lower():
            clean_df, dirty_rows = cleaner.clean_airlines_data(df)
            processor.insert_airlines(clean_df)
            processed_data = clean_df
            
        elif 'passenger' in filename.lower():
            existing_passengers = processor.get_existing_passengers()
            existing_keys = set(existing_passengers['passengerkey']) if not existing_passengers.empty else set()
            clean_df, dirty_rows = cleaner.clean_passengers_data(df, existing_keys)
            processor.insert_passengers(clean_df)
            processed_data = clean_df
            
        elif 'flight' in filename.lower():
            existing_airports = processor.get_existing_airports()
            clean_df, dirty_rows = cleaner.clean_flights_data(df, existing_airports)
            processor.insert_flights(clean_df)
            processed_data = clean_df
            
        elif 'sales' in filename.lower():
            existing_passengers = processor.get_existing_passengers()
            existing_flights = processor.get_existing_flights()
            existing_dates = processor.get_existing_dates()
            clean_df, dirty_rows = cleaner.clean_sales_data(df, existing_passengers, existing_flights, existing_dates)
            processor.insert_sales(clean_df)
            processed_data = clean_df
        
        if dirty_rows:
            processor.insert_dirty_data(dirty_rows, filename)
        
        result = {
            'message': f'Processed {filename}',
            'clean_rows': len(processed_data) if processed_data is not None else 0,
            'dirty_rows': len(dirty_rows),
            'filename': filename
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/check-eligibility', methods=['POST'])
def check_eligibility():
    try:
        data = request.json
        result = processor.check_insurance_eligibility(
            passenger_name=data.get('name'),
            flight_id=data.get('flightId'),
            baggage_status=data.get('baggage'),
            date=data.get('date')
        )
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'eligible': False, 'error': str(e)}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    try:
        # UPDATED: lowercase table names
        tables = ['dimairlines', 'dimairports', 'dimpassengers', 'dimflights', 'factsales', 'dirtydata']
        stats = {}
        for table in tables:
            response = processor._make_request(table)
            if response:
                stats[table] = len(response.json())
            else:
                stats[table] = 0
        return jsonify(stats), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    os.makedirs('uploads', exist_ok=True)
    print("🚀 Starting Airline Data Warehouse API...")
    print("📊 Supabase URL:", SUPABASE_URL)
    app.run(host='0.0.0.0', port=8000, debug=True)
