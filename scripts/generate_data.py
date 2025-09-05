#!/usr/bin/env python3
"""
Smart City IoT Data Generator

This script generates synthetic IoT sensor data for the SparkCity project.
Generates 5 different types of datasets in various formats as specified in the project requirements.
"""

import pandas as pd
import numpy as np
import json
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import random
import os
from pathlib import Path

# Configuration
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
NUM_DAYS = 7  # Generate 7 days of data
RECORDS_PER_HOUR = 60  # One record per minute for most sensors

# City boundaries (approximate bounds for a mid-size city)
CITY_LAT_MIN, CITY_LAT_MAX = 40.7000, 40.8000
CITY_LON_MIN, CITY_LON_MAX = -74.0200, -73.9000

class SmartCityDataGenerator:
    def __init__(self):
        self.start_time = datetime.now() - timedelta(days=NUM_DAYS)
        self.end_time = datetime.now()
        
        # Create data directory if it doesn't exist
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        print(f"🏙️ Generating Smart City IoT data...")
        print(f"📅 Date range: {self.start_time.strftime('%Y-%m-%d')} to {self.end_time.strftime('%Y-%m-%d')}")
        print(f"📁 Output directory: {DATA_DIR}")
        
    def generate_timestamps(self, interval_minutes=1):
        """Generate timestamp series for the data"""
        timestamps = []
        current = self.start_time
        while current < self.end_time:
            timestamps.append(current)
            current += timedelta(minutes=interval_minutes)
        return timestamps
    
    def generate_traffic_sensors(self):
        """Generate traffic sensor data (CSV format)"""
        print("🚗 Generating traffic sensor data...")
        
        # Define traffic sensor locations (major intersections)
        sensor_locations = [
            {"sensor_id": f"TRAFFIC_{i:03d}", 
             "lat": np.random.uniform(CITY_LAT_MIN, CITY_LAT_MAX),
             "lon": np.random.uniform(CITY_LON_MIN, CITY_LON_MAX),
             "road_type": np.random.choice(["highway", "arterial", "residential", "commercial"])}
            for i in range(1, 51)  # 50 traffic sensors
        ]
        
        timestamps = self.generate_timestamps(interval_minutes=5)  # Every 5 minutes
        
        traffic_data = []
        for timestamp in timestamps:
            for sensor in sensor_locations:
                # Simulate traffic patterns (higher during rush hours)
                hour = timestamp.hour
                is_rush_hour = (7 <= hour <= 9) or (17 <= hour <= 19)
                is_weekend = timestamp.weekday() >= 5
                
                base_count = 30 if sensor["road_type"] == "highway" else 15
                if is_rush_hour and not is_weekend:
                    vehicle_count = int(np.random.normal(base_count * 2, base_count * 0.3))
                    avg_speed = max(10, np.random.normal(25, 10))
                    congestion = "high" if vehicle_count > base_count * 1.5 else "medium"
                else:
                    vehicle_count = int(np.random.normal(base_count, base_count * 0.4))
                    avg_speed = np.random.normal(50, 15)
                    congestion = "low" if vehicle_count < base_count * 0.7 else "medium"
                
                traffic_data.append({
                    "sensor_id": sensor["sensor_id"],
                    "timestamp": timestamp.isoformat(),
                    "location_lat": sensor["lat"],
                    "location_lon": sensor["lon"],
                    "vehicle_count": max(0, vehicle_count),
                    "avg_speed": max(5, avg_speed),
                    "congestion_level": congestion,
                    "road_type": sensor["road_type"]
                })
        
        df = pd.DataFrame(traffic_data)
        output_path = DATA_DIR / "traffic_sensors.csv"
        df.to_csv(output_path, index=False)
        print(f"   ✅ Generated {len(traffic_data):,} traffic sensor records -> {output_path}")
        
    def generate_air_quality_data(self):
        """Generate air quality monitor data (JSON format)"""
        print("🌬️ Generating air quality monitor data...")
        
        # Define air quality monitor locations
        monitor_locations = [
            {"sensor_id": f"AQ_{i:03d}", 
             "lat": np.random.uniform(CITY_LAT_MIN, CITY_LAT_MAX),
             "lon": np.random.uniform(CITY_LON_MIN, CITY_LON_MAX)}
            for i in range(1, 21)  # 20 air quality monitors
        ]
        
        timestamps = self.generate_timestamps(interval_minutes=15)  # Every 15 minutes
        
        air_quality_data = []
        for timestamp in timestamps:
            for monitor in monitor_locations:
                # Simulate air quality patterns (worse during rush hours)
                hour = timestamp.hour
                is_rush_hour = (7 <= hour <= 9) or (17 <= hour <= 19)
                pollution_factor = 1.3 if is_rush_hour else 1.0
                
                air_quality_data.append({
                    "sensor_id": monitor["sensor_id"],
                    "timestamp": timestamp.isoformat(),
                    "location_lat": monitor["lat"],
                    "location_lon": monitor["lon"],
                    "pm25": max(0, np.random.normal(25 * pollution_factor, 8)),
                    "pm10": max(0, np.random.normal(40 * pollution_factor, 12)),
                    "no2": max(0, np.random.normal(30 * pollution_factor, 10)),
                    "co": max(0, np.random.normal(1.2 * pollution_factor, 0.4)),
                    "temperature": np.random.normal(20, 8),  # Celsius
                    "humidity": np.random.uniform(30, 80)  # Percentage
                })
        
        output_path = DATA_DIR / "air_quality.json"
        with open(output_path, 'w') as f:
            json.dump(air_quality_data, f, indent=2)
        print(f"   ✅ Generated {len(air_quality_data):,} air quality records -> {output_path}")
        
    def generate_weather_data(self):
        """Generate weather station data (Parquet format)"""
        print("🌤️ Generating weather station data...")
        
        # Define weather station locations
        station_locations = [
            {"station_id": f"WEATHER_{i:03d}", 
             "lat": np.random.uniform(CITY_LAT_MIN, CITY_LAT_MAX),
             "lon": np.random.uniform(CITY_LON_MIN, CITY_LON_MAX)}
            for i in range(1, 11)  # 10 weather stations
        ]
        
        timestamps = self.generate_timestamps(interval_minutes=30)  # Every 30 minutes
        
        weather_data = []
        base_temp = 20  # Base temperature in Celsius
        
        for timestamp in timestamps:
            # Simulate daily temperature variation
            hour_factor = np.sin((timestamp.hour - 6) * np.pi / 12) * 0.3
            day_temp = base_temp + np.random.normal(0, 3) + hour_factor * 5
            
            for station in station_locations:
                weather_data.append({
                    "station_id": station["station_id"],
                    "timestamp": timestamp.isoformat(),
                    "location_lat": station["lat"],
                    "location_lon": station["lon"],
                    "temperature": day_temp + np.random.normal(0, 1),
                    "humidity": max(20, min(100, np.random.normal(60, 15))),
                    "wind_speed": max(0, np.random.exponential(8)),
                    "wind_direction": np.random.uniform(0, 360),
                    "precipitation": max(0, np.random.exponential(0.5)) if np.random.random() < 0.1 else 0,
                    "pressure": np.random.normal(1013.25, 10)  # hPa
                })
        
        df = pd.DataFrame(weather_data)
        output_path = DATA_DIR / "weather_data.parquet"
        df.to_parquet(output_path, index=False)
        print(f"   ✅ Generated {len(weather_data):,} weather records -> {output_path}")
        
    def generate_energy_data(self):
        """Generate energy meter data (CSV format)"""
        print("⚡ Generating energy consumption data...")
        
        # Define energy meter locations
        building_types = ["residential", "commercial", "industrial", "office", "retail"]
        meter_locations = [
            {"meter_id": f"ENERGY_{i:04d}", 
             "lat": np.random.uniform(CITY_LAT_MIN, CITY_LAT_MAX),
             "lon": np.random.uniform(CITY_LON_MIN, CITY_LON_MAX),
             "building_type": np.random.choice(building_types)}
            for i in range(1, 201)  # 200 energy meters
        ]
        
        timestamps = self.generate_timestamps(interval_minutes=10)  # Every 10 minutes
        
        energy_data = []
        for timestamp in timestamps:
            for meter in meter_locations:
                # Simulate energy consumption patterns
                hour = timestamp.hour
                is_business_hours = 8 <= hour <= 18
                is_weekend = timestamp.weekday() >= 5
                
                # Base consumption by building type
                base_consumption = {
                    "residential": 3.0,
                    "commercial": 15.0,
                    "industrial": 50.0,
                    "office": 20.0,
                    "retail": 25.0
                }[meter["building_type"]]
                
                # Adjust for time patterns
                if meter["building_type"] in ["commercial", "office", "retail"]:
                    if is_business_hours and not is_weekend:
                        consumption_factor = 1.5
                    else:
                        consumption_factor = 0.3
                else:  # residential, industrial
                    consumption_factor = 1.0 if meter["building_type"] == "industrial" else (1.2 if 18 <= hour <= 22 else 0.8)
                
                power_consumption = base_consumption * consumption_factor * np.random.uniform(0.7, 1.3)
                voltage = np.random.normal(240, 5)  # Volts
                current = power_consumption / voltage * 1000  # Amps
                
                energy_data.append({
                    "meter_id": meter["meter_id"],
                    "timestamp": timestamp.isoformat(),
                    "building_type": meter["building_type"],
                    "location_lat": meter["lat"],
                    "location_lon": meter["lon"],
                    "power_consumption": power_consumption,
                    "voltage": voltage,
                    "current": current,
                    "power_factor": np.random.uniform(0.85, 0.95)
                })
        
        df = pd.DataFrame(energy_data)
        output_path = DATA_DIR / "energy_meters.csv"
        df.to_csv(output_path, index=False)
        print(f"   ✅ Generated {len(energy_data):,} energy consumption records -> {output_path}")
        
    def generate_city_zones(self):
        """Generate city zone reference data (CSV format)"""
        print("🏙️ Generating city zones reference data...")
        
        zones_data = [
            {"zone_id": "ZONE_001", "zone_name": "Downtown", "zone_type": "commercial", 
             "lat_min": 40.7200, "lat_max": 40.7400, "lon_min": -74.0100, "lon_max": -73.9900, "population": 25000},
            {"zone_id": "ZONE_002", "zone_name": "Financial District", "zone_type": "commercial", 
             "lat_min": 40.7000, "lat_max": 40.7200, "lon_min": -74.0200, "lon_max": -74.0000, "population": 15000},
            {"zone_id": "ZONE_003", "zone_name": "Residential North", "zone_type": "residential", 
             "lat_min": 40.7600, "lat_max": 40.8000, "lon_min": -74.0000, "lon_max": -73.9800, "population": 45000},
            {"zone_id": "ZONE_004", "zone_name": "Residential South", "zone_type": "residential", 
             "lat_min": 40.7000, "lat_max": 40.7200, "lon_min": -73.9800, "lon_max": -73.9600, "population": 38000},
            {"zone_id": "ZONE_005", "zone_name": "Industrial Park", "zone_type": "industrial", 
             "lat_min": 40.7400, "lat_max": 40.7600, "lon_min": -74.0200, "lon_max": -74.0000, "population": 5000},
            {"zone_id": "ZONE_006", "zone_name": "Tech Campus", "zone_type": "commercial", 
             "lat_min": 40.7600, "lat_max": 40.7800, "lon_min": -73.9800, "lon_max": -73.9600, "population": 12000},
            {"zone_id": "ZONE_007", "zone_name": "University Area", "zone_type": "mixed", 
             "lat_min": 40.7200, "lat_max": 40.7400, "lon_min": -73.9600, "lon_max": -73.9400, "population": 22000},
            {"zone_id": "ZONE_008", "zone_name": "Shopping District", "zone_type": "retail", 
             "lat_min": 40.7400, "lat_max": 40.7600, "lon_min": -73.9600, "lon_max": -73.9400, "population": 8000}
        ]
        
        df = pd.DataFrame(zones_data)
        output_path = DATA_DIR / "city_zones.csv"
        df.to_csv(output_path, index=False)
        print(f"   ✅ Generated {len(zones_data):,} city zone records -> {output_path}")
        
    def generate_all_data(self):
        """Generate all required datasets"""
        print("🚀 Starting Smart City data generation...\n")
        
        self.generate_traffic_sensors()
        self.generate_air_quality_data()
        self.generate_weather_data()
        self.generate_energy_data()
        self.generate_city_zones()
        
        print(f"\n✅ Data generation completed!")
        print(f"📁 All files saved to: {DATA_DIR}")
        print(f"📊 Generated datasets:")
        
        # Display file sizes
        for file_path in DATA_DIR.glob("*"):
            if file_path.is_file():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                print(f"   • {file_path.name}: {size_mb:.2f} MB")

def main():
    """Main execution function"""
    try:
        generator = SmartCityDataGenerator()
        generator.generate_all_data()
        
        print(f"\n🎉 Smart City IoT data generation successful!")
        print(f"💡 You can now proceed with the Spark notebooks and analysis.")
        
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        print(f"💡 Please install required packages: pip install pandas numpy pyarrow")
    except Exception as e:
        print(f"❌ Error generating data: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())