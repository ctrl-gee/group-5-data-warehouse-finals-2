import requests
import pandas as pd
import json
from datetime import datetime

class SupabaseProcessor:
    def __init__(self, supabase_url, supabase_key):
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
    
    def _make_request(self, endpoint, method='GET', data=None):
        url = f"{self.supabase_url}/rest/v1/{endpoint}"
        try:
            if method == 'GET':
                response = requests.get(url, headers=self.headers, params=data)
            elif method == 'POST':
                response = requests.post(url, headers=self.headers, json=data)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Supabase API error: {e}")
            return None
    
    def insert_airports(self, clean_df):
        for _, row in clean_df.iterrows():
            try:
                check_response = self._make_request('dimairports', 'GET', {'airportkey': f'eq.{row["AirportKey"]}'})
                existing_data = check_response.json() if check_response else []
                if not existing_data:
                    insert_data = {
                        'airportkey': row['AirportKey'],
                        'airportname': row['AirportName'],
                        'city': row['City'],
                        'country': row['Country'],
                        'region': row.get('Region', 'Unknown')
                    }
                    insert_response = self._make_request('dimairports', 'POST', insert_data)
                    if insert_response:
                        print(f"✅ Inserted airport: {row['AirportKey']} - {row['City']}, {row['Country']}")
            except Exception as e:
                print(f"❌ Error inserting airport {row['AirportKey']}: {str(e)}")
    
    def insert_airlines(self, clean_df):
        for _, row in clean_df.iterrows():
            try:
                check_response = self._make_request('dimairlines', 'GET', {'airlinekey': f'eq.{row["AirlineKey"]}'})
                existing_data = check_response.json() if check_response else []
                if not existing_data:
                    insert_data = {
                        'airlinekey': row['AirlineKey'],
                        'airlinename': row['AirlineName'],
                        'alliance': row.get('Alliance', 'Unknown')
                    }
                    self._make_request('dimairlines', 'POST', insert_data)
                    print(f"✅ Inserted airline: {row['AirlineKey']} - {row['AirlineName']}")
            except Exception as e:
                print(f"❌ Error inserting airline {row['AirlineKey']}: {str(e)}")
    
    def insert_passengers(self, clean_df):
        for _, row in clean_df.iterrows():
            try:
                check_response = self._make_request('dimpassengers', 'GET', {'passengerkey': f'eq.{row["PassengerKey"]}'})
                existing_data = check_response.json() if check_response else []
                
                if not existing_data:
                    insert_data = {
                        'passengerkey': row['PassengerKey'],
                        'fullname': row['FullName'],
                        'email': row.get('Email'),
                        'loyaltystatus': row.get('LoyaltyStatus', 'Bronze')
                    }
                    
                    self._make_request('dimpassengers', 'POST', insert_data)
                    print(f"✅ Inserted passenger: {row['PassengerKey']} - {row['FullName']} ({row.get('LoyaltyStatus', 'Bronze')})")
                        
            except Exception as e:
                print(f"❌ Error inserting passenger {row['PassengerKey']}: {str(e)}")
    
    def insert_flights(self, clean_df):
        for _, row in clean_df.iterrows():
            try:
                check_response = self._make_request('dimflights', 'GET', {'flightkey': f'eq.{row["FlightKey"]}'})
                existing_data = check_response.json() if check_response else []
                if not existing_data:
                    insert_data = {
                        'flightkey': row['FlightKey'],
                        'originairportkey': row['OriginAirportKey'],
                        'destinationairportkey': row['DestinationAirportKey'],
                        'aircrafttype': row.get('AircraftType', 'Unknown'),
                        'airlinekey': row.get('AirlineKey', 'Unknown')
                    }
                    self._make_request('dimflights', 'POST', insert_data)
                    print(f"✅ Inserted flight: {row['FlightKey']}")
            except Exception as e:
                print(f"❌ Error inserting flight {row['FlightKey']}: {str(e)}")
    
    def insert_sales(self, clean_df):
        for _, row in clean_df.iterrows():
            try:
                check_response = self._make_request('factsales', 'GET', {'transactionid': f'eq.{row["TransactionID"]}'})
                existing_data = check_response.json() if check_response else []
                if not existing_data:
                    insert_data = {
                        'transactionid': row['TransactionID'],
                        'datekey': row['DateKey'],
                        'passengerkey': row['PassengerKey'],
                        'flightkey': row['FlightKey'],
                        'ticketprice': float(row['TicketPrice']),
                        'taxes': float(row['Taxes']),
                        'baggagefees': float(row['BaggageFees']),
                        'totalamount': float(row['TotalAmount']),
                        'flightdelay': row.get('FlightDelay', 0),
                        'baggagestatus': row.get('BaggageStatus', 'Delivered'),
                        'iseligibleforinsurance': row['IsEligibleForInsurance']
                    }
                    self._make_request('factsales', 'POST', insert_data)
                    print(f"✅ Inserted sales transaction: {row['TransactionID']}")
            except Exception as e:
                print(f"❌ Error inserting sales {row['TransactionID']}: {str(e)}")
    
    def insert_dirty_data(self, dirty_rows, source_table):
        for dirty_row in dirty_rows:
            try:
                dirty_data = {
                    'originaldata': dirty_row['data'],
                    'errorreason': dirty_row['error'],
                    'sourcetable': source_table
                }
                self._make_request('dirtydata', 'POST', dirty_data)
                print(f"🚨 Inserted dirty data from {source_table}: {dirty_row['error']}")
            except Exception as e:
                print(f"❌ Error inserting dirty data: {str(e)}")
    
    def get_existing_airports(self):
        try:
            response = self._make_request('dimairports')
            return pd.DataFrame(response.json()) if response else pd.DataFrame()
        except Exception as e:
            print(f"Error getting airports: {str(e)}")
            return pd.DataFrame()
    
    def get_existing_passengers(self):
        try:
            response = self._make_request('dimpassengers')
            return pd.DataFrame(response.json()) if response else pd.DataFrame()
        except Exception as e:
            print(f"Error getting passengers: {str(e)}")
            return pd.DataFrame()
    
    def get_existing_flights(self):
        try:
            response = self._make_request('dimflights')
            return pd.DataFrame(response.json()) if response else pd.DataFrame()
        except Exception as e:
            print(f"Error getting flights: {str(e)}")
            return pd.DataFrame()
    
    def get_existing_dates(self):
        try:
            response = self._make_request('dimdate')
            return pd.DataFrame(response.json()) if response else pd.DataFrame()
        except Exception as e:
            print(f"Error getting dates: {str(e)}")
            return pd.DataFrame()
    
    def check_insurance_eligibility(self, passenger_name, flight_id, baggage_status, date):
        try:
            date_key = int(date.replace('-', ''))
            passenger_response = self._make_request('dimpassengers', 'GET', {'fullname': f'ilike.%{passenger_name}%'})
            if not passenger_response or not passenger_response.json():
                return {'eligible': False, 'reason': 'Passenger not found'}
            passenger_key = passenger_response.json()[0]['passengerkey']
            sales_response = self._make_request('factsales', 'GET', {
                'passengerkey': f'eq.{passenger_key}',
                'flightkey': f'eq.{flight_id}',
                'datekey': f'eq.{date_key}',
                'baggagestatus': f'eq.{baggage_status}'
            })
            if sales_response and sales_response.json():
                record = sales_response.json()[0]
                is_eligible = record['iseligibleforinsurance']
                if is_eligible:
                    reason = "Flight delay > 4 hours or baggage issues" 
                    if record.get('flightdelay', 0) > 240:
                        reason = f"Flight delayed by {record['flightdelay']} minutes"
                    elif record.get('baggagestatus') in ['Lost', 'Damaged']:
                        reason = f"Baggage {record['baggagestatus'].lower()}"
                    return {'eligible': True, 'reason': reason}
                else:
                    return {'eligible': False, 'reason': 'No qualifying insurance conditions met'}
            else:
                return {'eligible': False, 'reason': 'No matching flight record found'}
        except Exception as e:
            print(f"Error checking eligibility: {str(e)}")
            return {'eligible': False, 'reason': 'System error'}
