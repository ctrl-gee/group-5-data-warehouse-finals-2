import pandas as pd
import re
from datetime import datetime
import json

class DataCleaner:
    def __init__(self):
        self.known_countries = {
            'USA': 'United States', 'US': 'United States', 'U.S.A': 'United States', 'U.S.A.': 'United States', 'America': 'United States',
            'UK': 'United Kingdom', 'U.K': 'United Kingdom', 'UAE': 'United Arab Emirates'
        }
        
        self.known_cities = {
            'Honolulu': 'United States', 'New York City': 'United States', 'NYC': 'United States',
            'LA': 'United States', 'SF': 'United States', 'Chicago': 'United States'
        }
        
        # Valid pattern: P followed by 4-5 digits
        self.valid_passenger_pattern = r'^P\d{4,5}$'
        
        # Track the highest passenger number to ensure proper incrementing
        self.highest_passenger_number = 0
    
    def clean_passengers_data(self, df, existing_passenger_keys=None):
        """Clean passengers data with correct columns: PassengerKey, FullName, Email, LoyaltyStatus"""
        clean_rows = []
        dirty_rows = []
        
        # Get existing keys and find the highest number
        existing_keys = set(existing_passenger_keys) if existing_passenger_keys else set()
        self._find_highest_passenger_number(existing_keys)
        
        current_keys = set(existing_keys)
        
        for index, row in df.iterrows():
            try:
                original_data = row.to_dict()
                original_key = row.get('PassengerKey')
                
                # Clean and validate PassengerKey with advanced pattern recognition
                passenger_key = self.clean_and_generate_passenger_key_advanced(
                    original_key, 
                    current_keys,
                    index
                )
                
                if not passenger_key:
                    dirty_rows.append({
                        'data': original_data,
                        'error': f'Invalid PassengerKey: {original_key} - cannot generate valid key'
                    })
                    continue
                
                # Add to current keys to avoid duplicates in this batch
                current_keys.add(passenger_key)
                
                # Validate required fields
                if pd.isna(row.get('FullName')) or str(row.get('FullName')).strip() == '':
                    dirty_rows.append({
                        'data': original_data, 
                        'error': 'Missing passenger name'
                    })
                    continue
                
                # Clean name
                full_name = self.clean_name(str(row.get('FullName', '')))
                
                # Clean and validate email
                email = self.clean_email(str(row.get('Email', ''))) if not pd.isna(row.get('Email')) else None
                
                # Clean and validate loyalty status
                loyalty_status = self.clean_loyalty_status(str(row.get('LoyaltyStatus', '')).lower()) if not pd.isna(row.get('LoyaltyStatus')) else 'Bronze'
                
                clean_row = {
                    'PassengerKey': passenger_key,
                    'FullName': full_name,
                    'Email': email,
                    'LoyaltyStatus': loyalty_status
                }
                
                clean_rows.append(clean_row)
                
                # Show transformation if key was changed
                if str(original_key) != passenger_key:
                    print(f"🔄 Transformed '{original_key}' → '{passenger_key}'")
                
            except Exception as e:
                dirty_rows.append({
                    'data': row.to_dict(),
                    'error': f'Passenger processing error: {str(e)}'
                })
        
        return pd.DataFrame(clean_rows), dirty_rows

    def clean_email(self, email):
        """Basic email validation and cleaning"""
        if pd.isna(email) or email == '':
            return None
        
        email = str(email).strip().lower()
        
        # Basic email pattern check
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.match(email_pattern, email):
            return email
        else:
            print(f"⚠️ Invalid email format: {email}")
            return None

    def clean_loyalty_status(self, status):
        """Validate and standardize loyalty status"""
        status = str(status).strip().lower()
        
        valid_statuses = {
            'bronze': 'Bronze',
            'silver': 'Silver', 
            'gold': 'Gold',
            'platinum': 'Platinum',
            'b': 'Bronze',
            's': 'Silver',
            'g': 'Gold',
            'p': 'Platinum'
        }
        
        return valid_statuses.get(status, 'Bronze')  # Default to Bronze if invalid

    def _find_highest_passenger_number(self, existing_keys):
        """Find the highest passenger number from existing keys"""
        max_number = 0
        for key in existing_keys:
            if isinstance(key, str) and key.startswith('P'):
                # Extract numbers after 'P'
                numbers = re.findall(r'\d+', key[1:])
                if numbers:
                    current_num = int(numbers[0])
                    if current_num > max_number:
                        max_number = current_num
        self.highest_passenger_number = max_number
    
    def clean_and_generate_passenger_key_advanced(self, passenger_key, existing_keys, index):
        """
        Advanced passenger key cleaning with pattern recognition for complex invalid keys
        """
        if pd.isna(passenger_key) or passenger_key == '':
            return self.generate_incrementing_passenger_key(existing_keys)
        
        passenger_key = str(passenger_key).strip()
        
        # Check if already valid
        if self.is_valid_passenger_key(passenger_key) and passenger_key not in existing_keys:
            return passenger_key
        
        # Advanced pattern recognition for complex invalid keys
        transformed_key = self.transform_complex_passenger_key(passenger_key, existing_keys)
        
        if transformed_key and transformed_key not in existing_keys:
            return transformed_key
        
        # Generate new incrementing key as last resort
        return self.generate_incrementing_passenger_key(existing_keys)
    
    def is_valid_passenger_key(self, key):
        """Check if passenger key matches valid pattern (P followed by 4-5 digits)"""
        return bool(re.match(self.valid_passenger_pattern, key))
    
    def transform_complex_passenger_key(self, invalid_key, existing_keys):
        """
        Handle complex invalid patterns like:
        - P1L1592, P1VII-1798, P1P1937, P2Note: 2758
        - Extract numbers and transform to P1001, P1002 format
        """
        print(f"🔍 Analyzing complex key: '{invalid_key}'")
        
        # Remove spaces and convert to uppercase for consistent processing
        clean_key = invalid_key.replace(' ', '').upper()
        
        # Pattern 1: P1L1592 → Extract numbers after P: 1 and 1592
        match1 = re.match(r'^P(\d+)[A-Z]+(\d+)$', clean_key)
        if match1:
            num1, num2 = match1.groups()
            # Use the larger number or a combination
            candidate_num = max(int(num1), int(num2))
            candidate = f"P{candidate_num}"
            if self.is_valid_passenger_key(candidate) and candidate not in existing_keys:
                print(f"   Pattern 1: PXlettersY → P{max(int(num1), int(num2))}")
                return candidate
        
        # Pattern 2: P1VII-1798 → Extract numbers around Roman numerals and dash
        match2 = re.match(r'^P(\d+)[A-Z]+-(\d+)$', clean_key)
        if match2:
            num1, num2 = match2.groups()
            candidate_num = max(int(num1), int(num2))
            candidate = f"P{candidate_num}"
            if self.is_valid_passenger_key(candidate) and candidate not in existing_keys:
                print(f"   Pattern 2: PXletters-Y → P{max(int(num1), int(num2))}")
                return candidate
        
        # Pattern 3: P1P1937 → Already has P, extract the significant number
        match3 = re.match(r'^P(\d+)P?(\d+)$', clean_key)
        if match3:
            num1, num2 = match3.groups()
            # Use the second number as it's likely the significant one
            candidate = f"P{num2}"
            if self.is_valid_passenger_key(candidate) and candidate not in existing_keys:
                print(f"   Pattern 3: PXPX → P{num2}")
                return candidate
        
        # Pattern 4: P2Note: 2758 → Extract number after colon/space
        match4 = re.match(r'^P(\d+)[A-Z]+:?\s*(\d+)$', clean_key)
        if match4:
            num1, num2 = match4.groups()
            candidate = f"P{num2}"
            if self.is_valid_passenger_key(candidate) and candidate not in existing_keys:
                print(f"   Pattern 4: PXtext: Y → P{num2}")
                return candidate
        
        # Pattern 5: Extract all numbers and use the largest
        all_numbers = re.findall(r'\d+', clean_key)
        if all_numbers:
            largest_num = max(map(int, all_numbers))
            candidate = f"P{largest_num}"
            # Ensure it has 4-5 digits
            if len(str(largest_num)) < 4:
                candidate = f"P{largest_num:04d}"  # Pad to 4 digits
            if self.is_valid_passenger_key(candidate) and candidate not in existing_keys:
                print(f"   Pattern 5: Extract largest number → {candidate}")
                return candidate
        
        # Pattern 6: If it starts with P but has invalid format, try to salvage
        if clean_key.startswith('P'):
            # Extract first set of numbers after P
            numbers_after_p = re.findall(r'\d+', clean_key[1:])
            if numbers_after_p:
                candidate_num = int(numbers_after_p[0])
                candidate = f"P{candidate_num:04d}"  # Ensure 4 digits
                if self.is_valid_passenger_key(candidate) and candidate not in existing_keys:
                    print(f"   Pattern 6: Starts with P → {candidate}")
                    return candidate
        
        return None
    
    def generate_incrementing_passenger_key(self, existing_keys):
        """Generate the next passenger key in sequence P1001, P1002, etc."""
        # Start from the highest found number + 1, or from 1001 if no existing
        start_number = max(self.highest_passenger_number + 1, 1001)
        
        for attempt in range(1000):
            candidate = f"P{start_number + attempt}"
            if candidate not in existing_keys:
                self.highest_passenger_number = start_number + attempt
                return candidate
        
        # Fallback: use timestamp
        return f"P{10000 + int(datetime.now().timestamp() % 10000)}"
    
    def clean_airports_data(self, df):
        clean_rows = []
        dirty_rows = []
        
        for index, row in df.iterrows():
            try:
                if pd.isna(row.get('AirportKey')) or pd.isna(row.get('AirportName')) or pd.isna(row.get('City')):
                    dirty_rows.append({'data': row.to_dict(), 'error': 'Missing required fields'})
                    continue
                
                airport_key = str(row['AirportKey']).strip().upper()
                if not self.is_valid_airport_key(airport_key):
                    dirty_rows.append({
                        'data': row.to_dict(),
                        'error': f'Invalid AirportKey: {airport_key}. Must be 3 uppercase letters'
                    })
                    continue
                
                country = self.standardize_country(row.get('Country', ''))
                city = str(row['City']).strip()
                
                if pd.isna(country) or country == '':
                    if city in self.known_cities:
                        country = self.known_cities[city]
                    else:
                        country = self.infer_country_from_city(city)
                
                if not country:
                    dirty_rows.append({
                        'data': row.to_dict(),
                        'error': f'Cannot determine country for city: {city}'
                    })
                    continue
                
                clean_row = {
                    'AirportKey': airport_key,
                    'AirportName': str(row['AirportName']).strip(),
                    'City': city,
                    'Country': country,
                    'Region': self.get_region(country)
                }
                
                clean_rows.append(clean_row)
                
            except Exception as e:
                dirty_rows.append({'data': row.to_dict(), 'error': f'Processing error: {str(e)}'})
        
        return pd.DataFrame(clean_rows), dirty_rows
    
    def clean_airlines_data(self, df):
        clean_rows = []
        dirty_rows = []
        
        for index, row in df.iterrows():
            try:
                if pd.isna(row.get('AirlineKey')) or pd.isna(row.get('AirlineName')):
                    dirty_rows.append({'data': row.to_dict(), 'error': 'Missing required fields'})
                    continue
                
                airline_key = str(row['AirlineKey']).strip().upper()
                if not self.is_valid_airline_key(airline_key):
                    dirty_rows.append({
                        'data': row.to_dict(),
                        'error': f'Invalid AirlineKey: {airline_key}. Must be 2 uppercase letters'
                    })
                    continue
                
                clean_row = {
                    'AirlineKey': airline_key,
                    'AirlineName': str(row['AirlineName']).strip(),
                    'Alliance': str(row.get('Alliance', '')).strip()
                }
                
                clean_rows.append(clean_row)
                
            except Exception as e:
                dirty_rows.append({'data': row.to_dict(), 'error': f'Processing error: {str(e)}'})
        
        return pd.DataFrame(clean_rows), dirty_rows
    
    def clean_flights_data(self, df, existing_airports):
        clean_rows = []
        dirty_rows = []
        
        valid_airports = set(existing_airports['AirportKey']) if not existing_airports.empty else set()
        
        for index, row in df.iterrows():
            try:
                if pd.isna(row.get('FlightKey')) or pd.isna(row.get('OriginAirportKey')) or pd.isna(row.get('DestinationAirportKey')):
                    dirty_rows.append({'data': row.to_dict(), 'error': 'Missing required flight fields'})
                    continue
                
                origin = str(row['OriginAirportKey']).strip().upper()
                destination = str(row['DestinationAirportKey']).strip().upper()
                
                if origin not in valid_airports:
                    dirty_rows.append({'data': row.to_dict(), 'error': f'Origin airport not found: {origin}'})
                    continue
                    
                if destination not in valid_airports:
                    dirty_rows.append({'data': row.to_dict(), 'error': f'Destination airport not found: {destination}'})
                    continue
                
                flight_key = str(row['FlightKey']).strip()
                airline_key = flight_key[:2]
                
                clean_row = {
                    'FlightKey': flight_key,
                    'OriginAirportKey': origin,
                    'DestinationAirportKey': destination,
                    'AircraftType': str(row.get('AircraftType', 'Unknown')).strip(),
                    'AirlineKey': airline_key
                }
                
                clean_rows.append(clean_row)
                
            except Exception as e:
                dirty_rows.append({'data': row.to_dict(), 'error': f'Flight processing error: {str(e)}'})
        
        return pd.DataFrame(clean_rows), dirty_rows
    
    def clean_sales_data(self, df, existing_passengers, existing_flights, existing_dates):
        clean_rows = []
        dirty_rows = []
        
        valid_passengers = set(existing_passengers['PassengerKey']) if not existing_passengers.empty else set()
        valid_flights = set(existing_flights['FlightKey']) if not existing_flights.empty else set()
        valid_dates = set(existing_dates['DateKey']) if not existing_dates.empty else set()
        
        for index, row in df.iterrows():
            try:
                passenger_key = str(row['PassengerKey']).strip()
                flight_key = str(row['FlightKey']).strip()
                date_key = int(row['DateKey'])
                
                if passenger_key not in valid_passengers:
                    dirty_rows.append({'data': row.to_dict(), 'error': f'Passenger not found: {passenger_key}'})
                    continue
                    
                if flight_key not in valid_flights:
                    dirty_rows.append({'data': row.to_dict(), 'error': f'Flight not found: {flight_key}'})
                    continue
                    
                if date_key not in valid_dates:
                    dirty_rows.append({'data': row.to_dict(), 'error': f'Date not found: {date_key}'})
                    continue
                
                flight_delay = row.get('FlightDelay', 0)
                baggage_status = str(row.get('BaggageStatus', 'Delivered'))
                is_eligible = self.check_insurance_eligibility(flight_delay, baggage_status)
                
                clean_row = {
                    'TransactionID': int(row['TransactionID']),
                    'DateKey': date_key,
                    'PassengerKey': passenger_key,
                    'FlightKey': flight_key,
                    'TicketPrice': float(row['TicketPrice']),
                    'Taxes': float(row['Taxes']),
                    'BaggageFees': float(row['BaggageFees']),
                    'TotalAmount': float(row['TotalAmount']),
                    'FlightDelay': flight_delay,
                    'BaggageStatus': baggage_status,
                    'IsEligibleForInsurance': is_eligible
                }
                
                clean_rows.append(clean_row)
                
            except Exception as e:
                dirty_rows.append({'data': row.to_dict(), 'error': f'Sales processing error: {str(e)}'})
        
        return pd.DataFrame(clean_rows), dirty_rows
    
    def is_valid_airport_key(self, key):
        return bool(re.match(r'^[A-Z]{3}$', key))
    
    def is_valid_airline_key(self, key):
        return bool(re.match(r'^[A-Z]{2}$', key))
    
    def standardize_country(self, country):
        if pd.isna(country) or country == '':
            return ''
        country = str(country).strip()
        if country.upper() in self.known_countries:
            return self.known_countries[country.upper()]
        country_lower = country.lower()
        if 'usa' in country_lower or 'united states' in country_lower or 'america' in country_lower:
            return 'United States'
        elif 'uk' in country_lower or 'united kingdom' in country_lower or 'britain' in country_lower:
            return 'United Kingdom'
        elif 'uae' in country_lower or 'united arab emirates' in country_lower:
            return 'United Arab Emirates'
        return country.title()
    
    def infer_country_from_city(self, city):
        city_lower = city.lower()
        us_cities = ['new york', 'los angeles', 'chicago', 'houston', 'phoenix', 'philadelphia', 'san antonio', 'san diego', 'dallas', 'san jose', 'honolulu', 'miami', 'seattle', 'boston', 'atlanta']
        for us_city in us_cities:
            if us_city in city_lower:
                return 'United States'
        return ''
    
    def get_region(self, country):
        regions = {
            'United States': 'North America', 'Canada': 'North America',
            'United Kingdom': 'Europe', 'France': 'Europe', 'Germany': 'Europe',
            'Japan': 'Asia', 'China': 'Asia', 'Australia': 'Oceania'
        }
        return regions.get(country, 'Other')
    
    def clean_name(self, name):
        name = re.sub(r'[^a-zA-Z\s]', '', name)
        return ' '.join([part.capitalize() for part in name.split()])
    
    def check_insurance_eligibility(self, flight_delay, baggage_status):
        flight_eligible = flight_delay > 240
        baggage_eligible = baggage_status in ['Lost', 'Damaged']
        return flight_eligible or baggage_eligible
