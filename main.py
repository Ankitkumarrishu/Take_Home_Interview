from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import pytz
from datetime import datetime, timedelta
import uuid
import os
import tempfile
import csv
from typing import Dict, List
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

def import_csv_data():
    """Import CSV data into the database"""
    global MAX_TIMESTAMP
    
    db = SessionLocal()
    try:
        # Clear existing data
        db.query(StoreStatus).delete()
        db.query(StoreHours).delete()
        db.query(StoreTimezone).delete()
        
        # Import store_status.csv
        logger.info("Importing store_status.csv...")
        store_status_df = pd.read_csv('store_status.csv')
        store_status_df['timestamp_utc'] = pd.to_datetime(store_status_df['timestamp_utc'])
        
        # Find max timestamp for current time simulation
        MAX_TIMESTAMP = store_status_df['timestamp_utc'].max()
        logger.info(f"Max timestamp found: {MAX_TIMESTAMP}")
        
        for _, row in store_status_df.iterrows():
            store_status = StoreStatus(
                store_id=row['store_id'],
                timestamp_utc=row['timestamp_utc'],
                status=row['status']
            )
            db.add(store_status)
        
        # Import menu_hours.csv
        logger.info("Importing menu_hours.csv...")
        store_hours_df = pd.read_csv('menu_hours.csv')
        for _, row in store_hours_df.iterrows():
            store_hours = StoreHours(
                store_id=row['store_id'],
                day_of_week=row['dayOfWeek'],
                start_time_local=row['start_time_local'],
                end_time_local=row['end_time_local']
            )
            db.add(store_hours)
        
        # Import timezones.csv
        logger.info("Importing timezones.csv...")
        timezones_df = pd.read_csv('timezones.csv')
        for _, row in timezones_df.iterrows():
            store_timezone = StoreTimezone(
                store_id=row['store_id'],
                timezone_str=row['timezone_str']
            )
            db.add(store_timezone)
        
        db.commit()
        logger.info("Data import completed successfully")
        
    except Exception as e:
        logger.error(f"Error importing data: {e}")
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
    logger.info("Starting up and importing CSV data...")
    import_csv_data()

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
    uvicorn.run(app, host="127.0.0.1", port=8000)

