## :house: Extracting Airbnb data from inside airbnb
This process handles the ingestion of airbnb data in Bristol,Edinburgh,Greater Manchester,London. The script streams data directly from the inside Airbnb to our s3 data lake to ensure efficient memory usage for large-scale files.

## :hammer_and_wrench: Configuration Parameters
when running the extraction script, using the following parameters to define the source and destinations:

| parameter | Value | Description |
| :--- | :--- | :--- |
| url | ```https://data.insideairbnb.com/united-kingdom/england/greater-manchester/2025-09-26/data/listings.csv.gz``` | The source URL for either the full history or the monthly update
| bucket | ```quibbler-house-data-lake``` | The target S3 bucket where the raw data is stored.
| key | ```raw/airbnb/listings.csv.gz``` | The destination path and filename within the S3 bucket

## :bulb: Data Dataset Coverage
* **Manchester**
    * **URL 09 2025**: `https://data.insideairbnb.com/united-kingdom/england/greater-manchester/2025-09-26/data/listings.csv.gz`
    * **URL 06 2025**: `https://data.insideairbnb.com/united-kingdom/england/greater-manchester/2025-06-24/data/listings.csv.gz`
    * **URL 03 2025**: `https://data.insideairbnb.com/united-kingdom/england/greater-manchester/2025-03-18/data/listings.csv.gz`

* **London**
    * **09 2025**: `https://data.insideairbnb.com/united-kingdom/england/london/2025-09-14/data/listings.csv.gz`
    * **06 2025**: `https://data.insideairbnb.com/united-kingdom/england/london/2025-06-10/data/listings.csv.gz`
    * **03 2025**: `https://data.insideairbnb.com/united-kingdom/england/london/2025-03-04/data/listings.csv.gz`

* **Bristol**
    * **09 2025**:`https://data.insideairbnb.com/united-kingdom/england/bristol/2025-09-26/data/listings.csv.gz`
    * **06 2025**:`https://data.insideairbnb.com/united-kingdom/england/bristol/2025-06-24/data/listings.csv.gz`
    * **03 2025**:`https://data.insideairbnb.com/united-kingdom/england/bristol/2025-03-19/data/listings.csv.gz`
* **Edinburgh**
    * **09 2025**:`https://data.insideairbnb.com/united-kingdom/scotland/edinburgh/2025-09-21/data/listings.csv.gz`
    * **06 2025**:`https://data.insideairbnb.com/united-kingdom/scotland/edinburgh/2025-06-15/data/listings.csv.gz`
    * **03 2025**:`https://data.insideairbnb.com/united-kingdom/scotland/edinburgh/2025-03-08/data/listings.csv.gz`

