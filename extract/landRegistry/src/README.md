## :house: Extracting Land Registry Price Paid Data
This process handles the ingestion of residential property sale records in England and Wales. The script streams data directly from the Land Registry's public storage to our S3 data lake to ensure efficient memory usage for large-scale files.


## :hammer_and_wrench: Configuration Parameters

When running the extraction script, use the following parameters to define the source and destination:


| parameter | Value | Description |
| :--- | :--- | :--- |
| url | ```http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv``` | The source URL for either the full history or the monthly update
| bucket | ```quibbler-house-data-lake``` | The target S3 bucket where the raw data is stored.
| key | ```raw/land_registry/pp-complete.csv``` | The destination path and filename within the S3 bucket


## :bulb: Data Dataset Coverage
The Land Registry provides two primary CSV versions. The choice of URL depends on whether you are performing a full historical backfill or a routine incremental update.

* **Complete Dataset**
    * **File**: `pp-complete.csv`
    * **Coverage**: All residential property sales in England and Wales from **1995 to the present**.
    * **Use Case**: Initial data lake hydration or full data refreshes.
* **Monthly Update**
    * **File**: `pp-monthly-update-new-version.csv`
    * **Coverage**: Transactions processed within the **latest month** only.
    * **Use Case**: Regular incremental updates to keep the data lake current.

## :rocket: Usage
Execute the script via the command line using the following examples:
**Full History Sync:**
```bash
python extract/landRegistry/src/extract.py --url="http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv" --bucket="quibbler-house-data-lake" --key="raw/land_registry/pp-complete.csv"
```