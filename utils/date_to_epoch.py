from datetime import datetime



# Date to convert (e.g., October 10st, 2024)

test_date = datetime(2024, 10, 1)

epoch_time = int(test_date.timestamp()) 

print(f"Data {test_date} is {epoch_time}") 

test_date = datetime(2024, 11, 21)
epoch_time = int(test_date.timestamp()) 
print(f"Data {test_date} is {epoch_time}") 