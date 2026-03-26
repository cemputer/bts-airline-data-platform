# Flight Data Documentation

**Background:**
The data contained in the compressed file has been extracted from the Reporting Carrier On-Time Performance (1987-present) data table of the "On-Time" database from the TranStats data library.

## Selected Columns Data Dictionary

| Column Name | Description |
| :--- | :--- |
| **Year** | Year |
| **Month** | Month |
| **DayOfWeek** | Day of Week |
| **FlightDate** | Flight Date (yyyymmdd) |
| **Reporting_Airline** | Unique Carrier Code. When the same code has been used by multiple carriers, a numeric suffix is used for earlier users, for example, PA, PA(1), PA(2). Use this field for analysis across a range of years. |
| **Origin** | Origin Airport |
| **OriginCityName** | Origin Airport, City Name |
| **Dest** | Destination Airport |
| **DestCityName** | Destination Airport, City Name |
| **CRSDepTime** | CRS Departure Time (local time: hhmm) |
| **DepTime** | Actual Departure Time (local time: hhmm) |
| **DepDelay** | Difference in minutes between scheduled and actual departure time. Early departures show negative numbers. |
| **TaxiOut** | Taxi Out Time, in Minutes |
| **TaxiIn** | Taxi In Time, in Minutes |
| **CRSArrTime** | CRS Arrival Time (local time: hhmm) |
| **ArrTime** | Actual Arrival Time (local time: hhmm) |
| **ArrDelay** | Difference in minutes between scheduled and actual arrival time. Early arrivals show negative numbers. |
| **Cancelled** | Cancelled Flight Indicator (1=Yes) |
| **CancellationCode** | Specifies The Reason For Cancellation |
| **Diverted** | Diverted Flight Indicator (1=Yes) |
| **ActualElapsedTime** | Elapsed Time of Flight, in Minutes |
| **AirTime** | Flight Time, in Minutes |
| **Distance** | Distance between airports (miles) |
| **CarrierDelay** | Carrier Delay, in Minutes |
| **WeatherDelay** | Weather Delay, in Minutes |
| **NASDelay** | National Air System Delay, in Minutes |
| **SecurityDelay** | Security Delay, in Minutes |
| **LateAircraftDelay** | Late Aircraft Delay, in Minutes |