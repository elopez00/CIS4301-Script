import csv
import sqlite3
import uuid
from datetime import datetime

# define the database name
db_name = "car_listings.db"

# define csv file name
csv_name = "vehicles.csv"

# create a connection to the database
conn = sqlite3.connect(db_name)
c = conn.cursor()

# create the tables
print("Creating tables...")

print("Creating Manufacturer table...")
c.execute("""CREATE TABLE IF NOT EXISTS Manufacturer (
                ManID TEXT PRIMARY KEY,
                Name TEXT
            )""")

print("Creating Model table...")
c.execute("""CREATE TABLE IF NOT EXISTS Model (
                ModelID TEXT PRIMARY KEY,
                Name TEXT,
                Year INTEGER,
                ManID TEXT,
                FOREIGN KEY (ManID) REFERENCES Manufacturer (ManID)
            )""")

print("Creating Location table...")
c.execute("""CREATE TABLE IF NOT EXISTS Location (
                LocationID TEXT PRIMARY KEY,
                State TEXT,
                Region TEXT
            )""")

print("Creating Listing table...")
c.execute("""CREATE TABLE IF NOT EXISTS Listing (
                ListingID TEXT PRIMARY KEY,
                Date TEXT,
                Image TEXT,
                Price REAL,
                Mileage INTEGER,
                Description TEXT,
                ModelID TEXT,
                LocationID TEXT,
                FOREIGN KEY (ModelID) REFERENCES Model (ModelID),
                FOREIGN KEY (LocationID) REFERENCES Location (LocationID)
            )""")

print("Done creating tables.")

# define the columns to be used
manufacturer_cols = ['manufacturer']
model_cols = ['model', 'year']
location_cols = ['state', 'region']
listing_cols = ['posting_date', 'image_url', 'price', 'odometer', 'description']

# keep a map of manufacturer names to IDs
manufacturers = {}
models = {}
locations = {}

# read the CSV file and insert data into tables
count = 0
with open('vehicles.csv') as csvfile:
    print("Reading CSV file...")
    reader = csv.DictReader(csvfile)
    print("Starting Data Insertion...")
    for row in reader:
        # check if all required columns are present in the current row
        has_all_cols = True
        for col in manufacturer_cols + model_cols + location_cols + listing_cols:
            if not row[col] or len(row[col]) == 0:
                has_all_cols = False
                break
            
        if not has_all_cols: continue
        

        # generate unique IDs
        man_id = str(uuid.uuid4())
        model_id = str(uuid.uuid4())
        location_id = str(uuid.uuid4())
        listing_id = str(uuid.uuid4())        

        # insert manufacturer data
        if row['manufacturer'] not in manufacturers:
            manufacturers[row['manufacturer']] = True
            c.execute(
                "INSERT INTO Manufacturer VALUES (?, ?)",
                (man_id, row['manufacturer'])
            )

        # insert model data
        if (row['model'] + row['year']) not in models: 
            models[row['model'] + row['year']] = True
            c.execute(
                "INSERT INTO Model VALUES (?, ?, ?, ?)",
                (model_id, row['model'], int(row['year']), man_id)
            )

        # insert location data
        if (row['state'] + row['region']) not in locations:
            locations[row['state'] + row['region']] = True
            c.execute(
                "INSERT INTO Location VALUES (?, ?, ?)",
                (location_id, row['state'], row['region'])
            )

        # insert listing data
        c.execute(
            "INSERT INTO Listing VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                listing_id, row['posting_date'], row['image_url'],
                float(row['price']), int(row['odometer']),
                row['description'], model_id, location_id
            )
        )
        
        count += 1
        if count % 1000 == 0: print("Processed " + str(count) + " rows.")

print ("Done inserting data.")

# commit changes and close the connection
conn.commit()
conn.close()
