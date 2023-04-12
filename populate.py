import csv
import uuid
import cx_Oracle

# define the database name


sid = "orcl"

# define csv file name
csv_name = "vehicles.csv"

# Establish a connection to the database
dsn = cx_Oracle.makedsn(hostname, port, sid)
conn = cx_Oracle.connect(username, password, dsn)
c = conn.cursor()

# create the tables
print("Creating tables...")

print("Creating Manufacturer table...")
c.execute(
    """CREATE TABLE Manufacturer (
        ManID VARCHAR2(36) PRIMARY KEY,
        Name VARCHAR2(255)
    )"""
)

print("Creating Model table...")
c.execute(
    """CREATE TABLE Model (
        ModelID VARCHAR2(36) PRIMARY KEY,
        Name VARCHAR2(255),
        Year NUMBER,
        ManID VARCHAR2(36),
        FOREIGN KEY (ManID) REFERENCES Manufacturer(ManID)
    )"""
)

print("Creating Location table...")
c.execute(
    """CREATE TABLE Location (
        LocationID VARCHAR2(36) PRIMARY KEY,
        State VARCHAR2(2),
        Region VARCHAR2(255)
    )"""
)

print("Creating Listing table...")
c.execute(
    """CREATE TABLE IF NOT EXISTS Listing (
        ListingID VARCHAR2(36) PRIMARY KEY,
        Date TIMESTAMP,
        Image VARCHAR2(255),
        Price NUMBER,
        Mileage NUMBER,
        Description VARCHAR2(4000),
        ModelID VARCHAR2(36),
        LocationID VARCHAR2(36),
        FOREIGN KEY (ModelID) REFERENCES Model(ModelID),
        FOREIGN KEY (LocationID) REFERENCES Location(LocationID)
    )"""
)

print("Done creating tables.")

# define the columns to be used
manufacturer_cols = ["manufacturer"]
model_cols = ["model", "year"]
location_cols = ["state", "region"]
listing_cols = ["posting_date", "image_url", "price", "odometer", "description"]

# keep a map of manufacturer names to IDs
manufacturers = {}
models = {}
locations = {}

# read the CSV file and insert data into tables
count = 0
with open("vehicles.csv") as csvfile:
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

        if not has_all_cols:
            continue

        # generate unique IDs
        man_id = str(uuid.uuid4())
        model_id = str(uuid.uuid4())
        location_id = str(uuid.uuid4())
        listing_id = str(uuid.uuid4())

        # insert manufacturer data
        if row["manufacturer"] not in manufacturers:
            manufacturers[row["manufacturer"]] = True
            c.execute(
                "INSERT INTO Manufacturer (ManID, Name) VALUES (:man_id, :name)", 
                { 'man_id': man_id, 'name': row["manufacturer"]}
            )

        # insert model data
        if (row["model"] + row["year"]) not in models:
            models[row["model"] + row["year"]] = True
            c.execute(
                "INSERT INTO Model (ModelID, Name, Year, ManID) VALUES (:model_id, :name, :year, :man_id)",
                { 
                    'model_id': model_id, 
                    'name': row["model"], 
                    'year': int(row["year"]), 
                    'man_id': man_id
                },
            )

        # insert location data
        if (row["state"] + row["region"]) not in locations:
            locations[row["state"] + row["region"]] = True
            c.execute(
                "INSERT INTO Location VALUES (:location_id, :state, :region)",
                {
                    'location_id': location_id,
                    'state': row["state"],
                    'region': row["region"],
                },
            )

        # insert listing data
        c.execute(
            "INSERT INTO Listing VALUES (:listing_id, :date, :image, :price, :mileage, :description, :model_id, :location_id)",
            {
                'listing_id': listing_id,
                'date': row["posting_date"],
                'image': row["image_url"],
                'price': float(row["price"]),
                'mileage': int(row["odometer"]),
                'description': row["description"],
                'model_id': model_id,
                'location_id': location_id,
            },
        )

        count += 1
        if count % 1000 == 0:
            print("Processed " + str(count) + " rows.")

print("Done inserting data.")

# commit changes and close the connection
conn.commit()
conn.close()
