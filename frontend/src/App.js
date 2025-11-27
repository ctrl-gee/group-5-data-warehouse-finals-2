import React, { useState } from 'react';
import './App.css';

const API_BASE = 'http://localhost:8000';

function App() {
  const [activeTab, setActiveTab] = useState('upload');
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploadStatus, setUploadStatus] = useState('');
  const [searchParams, setSearchParams] = useState({
    name: '',
    flightId: '',
    baggage: 'Delivered',
    date: ''
  });
  const [eligibilityResult, setEligibilityResult] = useState(null);
  const [stats, setStats] = useState(null);

  const handleFileUpload = async (event) => {
    event.preventDefault();
    if (!selectedFile) {
      alert('Please select a file');
      return;
    }

    const formData = new FormData();
    formData.append('file', selectedFile);

    try {
      setUploadStatus('Uploading...');
      
      const response = await fetch(`${API_BASE}/upload`, {
        method: 'POST',
        body: formData,
      });

      const result = await response.json();
      
      if (response.ok) {
        setUploadStatus('File uploaded! Now processing...');
        
        // Process the file
        const processResponse = await fetch(`${API_BASE}/process`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ file_path: result.file_path }),
        });

        const processResult = await processResponse.json();
        
        if (processResponse.ok) {
          setUploadStatus(`✅ Success! ${processResult.clean_rows} clean rows processed, ${processResult.dirty_rows} moved to DirtyData`);
        } else {
          setUploadStatus('❌ Processing failed');
        }
      } else {
        setUploadStatus('❌ Upload failed');
      }
    } catch (error) {
      setUploadStatus('❌ Error uploading file');
      console.error('Upload error:', error);
    }
  };

  const handleSearchEligibility = async (event) => {
    event.preventDefault();
    
    try {
      const response = await fetch(`${API_BASE}/check-eligibility`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(searchParams),
      });

      const result = await response.json();
      setEligibilityResult(result);
    } catch (error) {
      console.error('Search error:', error);
      setEligibilityResult({ eligible: false, error: 'Search failed' });
    }
  };

  const loadStats = async () => {
    try {
      const response = await fetch(`${API_BASE}/stats`);
      const result = await response.json();
      setStats(result);
    } catch (error) {
      console.error('Error loading stats:', error);
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>Data Warehouse Finals by Group 5</h1>
        <nav>
          <button 
            className={activeTab === 'upload' ? 'active' : ''}
            onClick={() => setActiveTab('upload')}
          >
            📤 Data Upload
          </button>
          <button 
            className={activeTab === 'eligibility' ? 'active' : ''}
            onClick={() => setActiveTab('eligibility')}
          >
            📋 Insurance Eligibility
          </button>
          <button 
            className={activeTab === 'stats' ? 'active' : ''}
            onClick={() => { setActiveTab('stats'); loadStats(); }}
          >
            📊 Statistics
          </button>
        </nav>
      </header>

      <main className="App-main">
        {activeTab === 'upload' && (
          <div className="upload-section">
            <h2>Upload New Data</h2>
            <p className="instruction">
              Upload CSV files for: Airlines, Airports, Passengers, Flights, or Sales data.
              The system will automatically clean and validate your data.
            </p>
            
            <form onSubmit={handleFileUpload}>
              <div className="form-group">
                <label>Select CSV File:</label>
                <input
                  type="file"
                  accept=".csv"
                  onChange={(e) => setSelectedFile(e.target.files[0])}
                />
              </div>
              <button type="submit" className="btn-primary">
                Upload & Process File
              </button>
            </form>
            
            {uploadStatus && (
              <div className={`status-message ${uploadStatus.includes('✅') ? 'success' : 'error'}`}>
                {uploadStatus}
              </div>
            )}

            <div className="info-box">
              <h3>💡 How it works:</h3>
              <ul>
                <li>✅ Clean data → Goes to appropriate tables</li>
                <li>🔄 Invalid keys (like "P1L1592") → Auto-transformed to valid format</li>
                <li>🚨 Dirty data → Moved to DirtyData table with error reasons</li>
                <li>📧 Email validation and LoyaltyStatus standardization</li>
              </ul>
            </div>
          </div>
        )}

        {activeTab === 'eligibility' && (
          <div className="eligibility-section">
            <h2>Insurance Eligibility Check</h2>
            <p className="instruction">
              Check if a passenger is eligible for insurance based on flight delays or baggage issues.
            </p>
            
            <form onSubmit={handleSearchEligibility}>
              <div className="form-group">
                <label>Passenger Name:</label>
                <input
                  type="text"
                  value={searchParams.name}
                  onChange={(e) => setSearchParams({
                    ...searchParams,
                    name: e.target.value
                  })}
                  placeholder="Enter full name"
                  required
                />
              </div>
              
              <div className="form-group">
                <label>Flight ID:</label>
                <input
                  type="text"
                  value={searchParams.flightId}
                  onChange={(e) => setSearchParams({
                    ...searchParams,
                    flightId: e.target.value
                  })}
                  placeholder="e.g., AA100"
                  required
                />
              </div>
              
              <div className="form-group">
                <label>Baggage Status:</label>
                <select
                  value={searchParams.baggage}
                  onChange={(e) => setSearchParams({
                    ...searchParams,
                    baggage: e.target.value
                  })}
                  required
                >
                  <option value="Delivered">Delivered</option>
                  <option value="Lost">Lost</option>
                  <option value="Damaged">Damaged</option>
                </select>
              </div>
              
              <div className="form-group">
                <label>Flight Date:</label>
                <input
                  type="date"
                  value={searchParams.date}
                  onChange={(e) => setSearchParams({
                    ...searchParams,
                    date: e.target.value
                  })}
                  required
                />
              </div>
              
              <button type="submit" className="btn-primary">
                Check Eligibility
              </button>
            </form>

            {eligibilityResult && (
              <div className={`result-popup ${eligibilityResult.eligible ? 'eligible' : 'not-eligible'}`}>
                <h3>
                  {eligibilityResult.eligible 
                    ? '✅ Customer is ELIGIBLE for Insurance' 
                    : '❌ Customer is NOT ELIGIBLE for Insurance'
                  }
                </h3>
                {eligibilityResult.reason && (
                  <p><strong>Reason:</strong> {eligibilityResult.reason}</p>
                )}
              </div>
            )}

            <div className="info-box">
              <h3>📋 Eligibility Conditions:</h3>
              <ul>
                <li>⏰ Flight delayed by more than 4 hours</li>
                <li>🎒 Baggage is lost</li>
                <li>📦 Baggage is damaged</li>
                <li>✅ Any one condition satisfied makes customer eligible</li>
              </ul>
            </div>
          </div>
        )}

        {activeTab === 'stats' && (
          <div className="stats-section">
            <h2>Data Warehouse Statistics</h2>
            
            <button onClick={loadStats} className="btn-secondary">
              Refresh Stats
            </button>

            {stats ? (
              <div className="stats-grid">
                <div className="stat-card">
                  <h3>✈️ Airlines</h3>
                  <p className="stat-number">{stats.DimAirlines || 0}</p>
                </div>
                <div className="stat-card">
                  <h3>🏢 Airports</h3>
                  <p className="stat-number">{stats.DimAirports || 0}</p>
                </div>
                <div className="stat-card">
                  <h3>👥 Passengers</h3>
                  <p className="stat-number">{stats.DimPassengers || 0}</p>
                </div>
                <div className="stat-card">
                  <h3>🛫 Flights</h3>
                  <p className="stat-number">{stats.DimFlights || 0}</p>
                </div>
                <div className="stat-card">
                  <h3>💰 Sales</h3>
                  <p className="stat-number">{stats.FactSales || 0}</p>
                </div>
                <div className="stat-card">
                  <h3>🚨 Dirty Data</h3>
                  <p className="stat-number">{stats.DirtyData || 0}</p>
                </div>
              </div>
            ) : (
              <p>Loading statistics...</p>
            )}
          </div>
        )}
      </main>
    </div>
  );
}

export default App;
