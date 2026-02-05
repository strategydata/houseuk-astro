## :house: Extracting EPC data from Open Data Communities
This process handles the ingestion of house EPC dataset. The script streams data directly from the inside Airbnb to our s3 data lake to ensure efficient memory usage for large-scale files.

## :hammer_and_wrench: Configuration Parameters
when running the extraction script, using the following parameters to define the source and destinations:

| parameter | Value | Description |
| :--- | :--- | :--- |
| url | ```https://epc.opendatacommunities.org/files/domestic-2025.zip``` | The source URL for yearly updated
| bucket | ```quibbler-house-data-lake``` | The target S3 bucket where the raw data is stored.
| key | ```raw/epc/domestic-2025.zip``` | The destination path and filename within the S3 bucket

## :bulb: Data Dataset Coverage
The Open Data communities maintains a robust archive of historical data. You can programmatically access these files by modifying the year in the URL.
- **Available Range**: 2008 to 2025.
- **format**:zip
- **Naming Convention**: `domestic-YYYY.zip`


## :rocket: Usage
Execute the script to access data by `https://epc.opendatacommunities.org/files/domestic-2025.zip`