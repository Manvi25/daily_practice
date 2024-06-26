#string to datetime
from datetime import datetime

date_string = "2024-06-26 14:30:00"

date_format = "%Y-%m-%d %H:%M:%S"

date_object = datetime.strptime(date_string, date_format)

print("Datetime object:", date_object)
print("Year:", date_object.year)
print("Month:", date_object.month)
print("Day:", date_object.day)
print("Hour:", date_object.hour)
print("Minute:", date_object.minute)
print("Second:", date_object.second)


#datetime to string

now = datetime.now()

date_format = "%Y-%m-%d %H:%M:%S"

date_string = now.strftime(date_format)

print("Date string:", date_string)

specific_datetime = datetime(2024, 6, 26, 14, 30, 0)
specific_date_string = specific_datetime.strftime(date_format)

print("Specific date string:", specific_date_string)
