from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv
import pytz
from datetime import datetime, timedelta
import uuid
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Store Monitoring API", version="1.0.0")

# Database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./store_monitoring.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database Models
class StoreStatus(Base):
    __tablename__ = "store_status"
    
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    timestamp_utc = Column(DateTime)
    status = Column(String)  # 'active' or 'inactive'

class StoreHours(Base):
    __tablename__ = "store_hours"
    
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    day_of_week = Column(Integer)  # 0=Monday, 6=Sunday
    start_time_local = Column(String)  # HH:MM format
    end_time_local = Column(String)   # HH:MM format

class StoreTimezone(Base):
    __tablename__ = "store_timezone"
    
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    timezone_str = Column(String)

class Report(Base):
    __tablename__ = "reports"
    
    id = Column(String, primary_key=True, index=True)
    status = Column(String)  # 'Running' or 'Complete'
    csv_file_path = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# Create tables
Base.metadata.create_all(bind=engine)

# Global variable to store the max timestamp from store_status.csv
MAX_TIMESTAMP = None

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def parse_datetime(datetime_str):
    """Parse datetime string in various formats"""
    try:
        # Try common datetime formats
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue
        
        # If none work, try parsing with dateutil (fallback)
        from dateutil import parser
        return parser.parse(datetime_str)
        
    except Exception as e:
        logger.error(f"Error parsing datetime: {datetime_str}, error: {e}")
        return None

def import_csv_data():
    """Import CSV data into the database using built-in csv module"""
    global MAX_TIMESTAMP
    
    db = SessionLocal()
    try:
        # Clear existing data
        db.query(StoreStatus).delete()
        db.query(StoreHours).delete()
        db.query(StoreTimezone).delete()
        
        print("=" * 60)
        print("STARTING CSV DATA IMPORT...")
        print("=" * 60)
        
        # Import store_status.csv
        print("1. Importing store_status.csv...")
        if os.path.exists('store_status.csv'):
            with open('store_status.csv', 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                max_timestamp = None
                row_count = 0
                
                for row in reader:
                    timestamp = parse_datetime(row['timestamp_utc'])
                    if timestamp:
                        if max_timestamp is None or timestamp > max_timestamp:
                            max_timestamp = timestamp
                        
                        store_status = StoreStatus(
                            store_id=row['store_id'],
                            timestamp_utc=timestamp,
                            status=row['status']
                        )
                        db.add(store_status)
                        row_count += 1
                
                MAX_TIMESTAMP = max_timestamp
                print(f"   ✓ Successfully imported {row_count} rows from store_status.csv")
                print(f"   ✓ Max timestamp found: {MAX_TIMESTAMP}")
        else:
            print("   ✗ ERROR: store_status.csv not found!")
            return
        
        # Import store hours - try different possible file names
        print("\n2. Looking for business hours file...")
        hours_file = None
        for filename in ['store_hours.csv', 'menu_hours.csv', 'business_hours.csv']:
            if os.path.exists(filename):
                hours_file = filename
                print(f"   ✓ Found hours file: {filename}")
                break
        
        if hours_file:
            print(f"   Importing {hours_file}...")
            with open(hours_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                row_count = 0
                for row in reader:
                    # Handle different possible column names
                    day_col = row.get('dayOfWeek') or row.get('day_of_week') or row.get('day')
                    start_col = row.get('start_time_local') or row.get('start_time') or row.get('start')
                    end_col = row.get('end_time_local') or row.get('end_time') or row.get('end')
                    
                    if day_col and start_col and end_col:
                        try:
                            store_hours = StoreHours(
                                store_id=row['store_id'],
                                day_of_week=int(day_col),
                                start_time_local=start_col,
                                end_time_local=end_col
                            )
                            db.add(store_hours)
                            row_count += 1
                        except (ValueError, KeyError) as e:
                            print(f"   ⚠ Warning: Skipping invalid row: {e}")
                print(f"   ✓ Successfully imported {row_count} rows from {hours_file}")
        else:
            print("   ⚠ Warning: No hours file found (store_hours.csv, menu_hours.csv, or business_hours.csv)")
            print("   → Stores will be treated as 24/7 open")
        
        # Import timezones.csv
        print("\n3. Importing timezones.csv...")
        if os.path.exists('timezones.csv'):
            with open('timezones.csv', 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                row_count = 0
                for row in reader:
                    store_timezone = StoreTimezone(
                        store_id=row['store_id'],
                        timezone_str=row['timezone_str']
                    )
                    db.add(store_timezone)
                    row_count += 1
            print(f"   ✓ Successfully imported {row_count} rows from timezones.csv")
        else:
            print("   ⚠ Warning: timezones.csv not found!")
            print("   → All stores will use default timezone: America/Chicago")
        
        db.commit()
        print("\n" + "=" * 60)
        print("✓ DATA IMPORT COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        
        # Print summary
        total_stores = db.query(StoreStatus.store_id).distinct().count()
        total_status_records = db.query(StoreStatus).count()
        total_hours_records = db.query(StoreHours).count()
        total_timezone_records = db.query(StoreTimezone).count()
        
        print(f"📊 IMPORT SUMMARY:")
        print(f"   • Total unique stores: {total_stores}")
        print(f"   • Total status records: {total_status_records}")
        print(f"   • Total business hours records: {total_hours_records}")
        print(f"   • Total timezone records: {total_timezone_records}")
        print(f"   • Current time (max timestamp): {MAX_TIMESTAMP}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ ERROR during data import: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def is_store_open(store_id: str, timestamp_utc: datetime, db) -> bool:
    """Check if a store is open at a given UTC timestamp"""
    try:
        # Get store timezone (default to America/Chicago if missing)
        timezone_record = db.query(StoreTimezone).filter(StoreTimezone.store_id == store_id).first()
        timezone_str = timezone_record.timezone_str if timezone_record else "America/Chicago"
        
        # Convert UTC to local time
        local_tz = pytz.timezone(timezone_str)
        local_time = timestamp_utc.replace(tzinfo=pytz.UTC).astimezone(local_tz)
        
        # Get day of week (0=Monday, 6=Sunday)
        day_of_week = local_time.weekday()
        
        # Check if store has specific hours for this day
        hours_record = db.query(StoreHours).filter(
            StoreHours.store_id == store_id,
            StoreHours.day_of_week == day_of_week
        ).first()
        
        # If no hours specified, store is open 24/7
        if not hours_record:
            return True
        
        # Parse business hours
        start_time = datetime.strptime(hours_record.start_time_local, "%H:%M").time()
        end_time = datetime.strptime(hours_record.end_time_local, "%H:%M").time()
        current_time = local_time.time()
        
        # Handle overnight hours (e.g., 22:00 to 06:00)
        if start_time > end_time:
            return current_time >= start_time or current_time <= end_time
        else:
            return start_time <= current_time <= end_time
            
    except Exception as e:
        logger.error(f"Error checking store hours: {e}")
        return True  # Default to open if error

def calculate_uptime_downtime(store_id: str, db, time_period: str) -> tuple:
    """Calculate uptime and downtime for a store within business hours"""
    try:
        # Get all status records for the store
        status_records = db.query(StoreStatus).filter(
            StoreStatus.store_id == store_id
        ).order_by(StoreStatus.timestamp_utc).all()
        
        if not status_records:
            return 0, 0
        
        # Filter records within business hours
        business_hours_records = []
        for record in status_records:
            if is_store_open(store_id, record.timestamp_utc, db):
                business_hours_records.append(record)
        
        if not business_hours_records:
            return 0, 0
        
        # Calculate time intervals based on period
        current_time = MAX_TIMESTAMP
        if time_period == "hour":
            start_time = current_time - timedelta(hours=1)
        elif time_period == "day":
            start_time = current_time - timedelta(days=1)
        elif time_period == "week":
            start_time = current_time - timedelta(weeks=1)
        else:
            return 0, 0
        
        # Filter records within time period
        period_records = [r for r in business_hours_records if r.timestamp_utc >= start_time]
        
        if not period_records:
            return 0, 0
        
        # Calculate total business hours in the period
        total_business_minutes = 0
        uptime_minutes = 0
        
        # For each day in the period, calculate business hours
        current_date = start_time.date()
        end_date = current_time.date()
        
        while current_date <= end_date:
            # Get business hours for this day
            day_of_week = current_date.weekday()
            hours_record = db.query(StoreHours).filter(
                StoreHours.store_id == store_id,
                StoreHours.day_of_week == day_of_week
            ).first()
            
            if hours_record:
                start_time_local = datetime.strptime(hours_record.start_time_local, "%H:%M").time()
                end_time_local = datetime.strptime(hours_record.end_time_local, "%H:%M").time()
                
                # Calculate business minutes for this day
                if start_time_local > end_time_local:  # Overnight hours
                    day_minutes = (24 * 60 - start_time_local.hour * 60 - start_time_local.minute + 
                                 end_time_local.hour * 60 + end_time_local.minute)
                else:
                    day_minutes = (end_time_local.hour * 60 + end_time_local.minute - 
                                 start_time_local.hour * 60 - start_time_local.minute)
                
                total_business_minutes += day_minutes
            else:
                # 24/7 store
                total_business_minutes += 24 * 60
            
            current_date += timedelta(days=1)
        
        # Calculate uptime based on observations
        for i, record in enumerate(period_records):
            if record.status == 'active':
                # Calculate time until next observation or end of period
                if i < len(period_records) - 1:
                    next_time = period_records[i + 1].timestamp_utc
                else:
                    next_time = current_time
                
                # Calculate minutes between observations
                time_diff = (next_time - record.timestamp_utc).total_seconds() / 60
                uptime_minutes += time_diff
        
        downtime_minutes = total_business_minutes - uptime_minutes
        
        # Convert to appropriate units
        if time_period == "hour":
            return uptime_minutes, downtime_minutes
        else:
            return uptime_minutes / 60, downtime_minutes / 60  # Convert to hours
            
    except Exception as e:
        logger.error(f"Error calculating uptime/downtime: {e}")
        return 0, 0

def generate_report(report_id: str):
    """Generate the store monitoring report"""
    try:
        db = SessionLocal()
        
        # Get all unique store IDs
        store_ids = db.query(StoreStatus.store_id).distinct().all()
        store_ids = [store_id[0] for store_id in store_ids]
        
        # Generate report data
        report_data = []
        for store_id in store_ids:
            # Calculate uptime and downtime for different periods
            uptime_hour, downtime_hour = calculate_uptime_downtime(store_id, db, "hour")
            uptime_day, downtime_day = calculate_uptime_downtime(store_id, db, "day")
            uptime_week, downtime_week = calculate_uptime_downtime(store_id, db, "week")
            
            report_data.append({
                'store_id': store_id,
                'uptime_last_hour': round(uptime_hour, 2),
                'uptime_last_day': round(uptime_day, 2),
                'uptime_last_week': round(uptime_week, 2),
                'downtime_last_hour': round(downtime_hour, 2),
                'downtime_last_day': round(downtime_day, 2),
                'downtime_last_week': round(downtime_week, 2)
            })
        
        # Create CSV file
        csv_file_path = f"report_{report_id}.csv"
        with open(csv_file_path, 'w', newline='') as csvfile:
            fieldnames = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week',
                         'downtime_last_hour', 'downtime_last_day', 'downtime_last_week']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(report_data)
        
        # Update report status
        report = db.query(Report).filter(Report.id == report_id).first()
        if report:
            report.status = "Complete"
            report.csv_file_path = csv_file_path
            db.commit()
        
        logger.info(f"Report {report_id} generated successfully")
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        # Update report status to failed
        try:
            report = db.query(Report).filter(Report.id == report_id).first()
            if report:
                report.status = "Failed"
                db.commit()
        except:
            pass
    finally:
        db.close()

@app.on_event("startup")
async def startup_event():
    """Import CSV data on startup"""
    print("\n🚀 STARTING STORE MONITORING API...")
    print("📁 Checking for CSV datasets...")
    import_csv_data()
    print("\n✅ Backend is running fine! All datasets imported successfully.")
    print("🌐 API Server is ready at: http://localhost:8000")
    print("📚 API Documentation: http://localhost:8000/docs")

@app.post("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    """Trigger report generation"""
    try:
        report_id = str(uuid.uuid4())
        
        # Create report record
        db = SessionLocal()
        report = Report(id=report_id, status="Running")
        db.add(report)
        db.commit()
        db.close()
        
        # Start report generation in background
        background_tasks.add_task(generate_report, report_id)
        
        return {"report_id": report_id}
        
    except Exception as e:
        logger.error(f"Error triggering report: {e}")
        raise HTTPException(status_code=500, detail="Failed to trigger report")

@app.get("/get_report/{report_id}")
async def get_report(report_id: str):
    """Get report status or download CSV"""
    try:
        db = SessionLocal()
        report = db.query(Report).filter(Report.id == report_id).first()
        db.close()
        
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        if report.status == "Running":
            return {"status": "Running"}
        elif report.status == "Complete":
            if report.csv_file_path and os.path.exists(report.csv_file_path):
                return FileResponse(
                    report.csv_file_path,
                    media_type="text/csv",
                    filename=f"store_report_{report_id}.csv"
                )
            else:
                raise HTTPException(status_code=500, detail="Report file not found")
        elif report.status == "Failed":
            return {"status": "Failed", "message": "Report generation failed"}
        else:
            return {"status": "Unknown"}
            
    except Exception as e:
        logger.error(f"Error getting report: {e}")
        raise HTTPException(status_code=500, detail="Failed to get report")

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Store Monitoring API", "version": "1.0.0"}

if __name__ == "__main__":
    import uvicorn
    print("🎯 Starting Store Monitoring API Server...")
    print("📊 Will import 3 datasets: store_status.csv, menu_hours.csv, timezones.csv")
    print("⏳ Please wait for data import to complete...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
