# Store Monitoring API

A FastAPI-based backend system for monitoring restaurant store uptime and downtime during business hours.

## Features

- **Data Import**: Automatically imports CSV data from `store_status.csv`, `menu_hours.csv`, and `timezones.csv`
- **Business Hours Calculation**: Considers store-specific business hours and timezones
- **Uptime/Downtime Monitoring**: Calculates store availability for last hour, day, and week
- **Report Generation**: Asynchronous report generation with background processing
- **CSV Export**: Downloads generated reports in CSV format

## Data Sources

The system works with three CSV files:

1. **store_status.csv**: Contains store activity data (`store_id`, `timestamp_utc`, `status`)
2. **menu_hours.csv**: Contains business hours (`store_id`, `dayOfWeek`, `start_time_local`, `end_time_local`)
3. **timezones.csv**: Contains store timezones (`store_id`, `timezone_str`)

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure your CSV files are in the project root directory:
   - `store_status.csv`
   - `menu_hours.csv`
   - `timezones.csv`

## Usage

### Starting the Server

```bash
python main.py
```

The server will start on `http://localhost:8000` and automatically import your CSV data.

### API Endpoints

#### 1. Trigger Report Generation
```http
POST /trigger_report
```

**Response:**
```json
{
  "report_id": "uuid-string-here"
}
```

#### 2. Get Report Status/Download
```http
GET /get_report/{report_id}
```

**Responses:**

- **Running**: `{"status": "Running"}`
- **Complete**: Downloads CSV file
- **Failed**: `{"status": "Failed", "message": "Report generation failed"}`

### Report Schema

The generated CSV contains:
- `store_id`: Unique identifier for the store
- `uptime_last_hour`: Uptime in minutes for the last hour
- `uptime_last_day`: Uptime in hours for the last day
- `uptime_last_week`: Uptime in hours for the last week
- `downtime_last_hour`: Downtime in minutes for the last hour
- `downtime_last_day`: Downtime in hours for the last day
- `downtime_last_week`: Downtime in hours for the last week

## Business Logic

- **Business Hours**: Only considers uptime/downtime during store business hours
- **Timezone Handling**: Converts UTC timestamps to local store timezones
- **24/7 Stores**: If no business hours specified, assumes store is open 24/7
- **Default Timezone**: Missing timezone data defaults to "America/Chicago"
- **Interpolation**: Extrapolates uptime/downtime based on periodic observations

## Example Workflow

1. **Start the server**: `python main.py`
2. **Trigger report**: `POST /trigger_report` → Get `report_id`
3. **Check status**: `GET /get_report/{report_id}` → Returns "Running" initially
4. **Wait and check again**: `GET /get_report/{report_id}` → Downloads CSV when complete

## Database

The system uses SQLite for data storage with the following tables:
- `store_status`: Store activity records
- `store_hours`: Business hours configuration
- `store_timezone`: Store timezone information
- `reports`: Report generation status tracking

## Error Handling

- Comprehensive logging for debugging
- Graceful error handling with appropriate HTTP status codes
- Background task failure tracking
- Data validation and sanitization

## Performance Considerations

- Background task processing for report generation
- Efficient database queries with proper indexing
- Minimal memory usage during data import
- Scalable architecture for future enhancements

