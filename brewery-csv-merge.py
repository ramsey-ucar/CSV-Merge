# acquired from https://pythonhosted.org/brewery/examples/merge_multiple_files.html
# discovered brewery is no longer being maintained
@author: Stefan Urbanek

import brewery
from brewery import ds
import sys

# Create source list

# Create list of fields 

sources = [
    {"file": "grants_2008.csv",
     "fields": ["receiver", "amount", "date"]},

    {"file": "grants_2009.csv",
     "fields": ["id", "receiver", "amount", "contract_number", "date"]},

    {"file": "grants_2010.csv",
     "fields": ["receiver", "subject", "requested_amount", "amount", "date"]}
]

# Create list of all fields and add filename to store information
# about origin of data records
all_fields = brewery.FieldList(["file"])

# Go through source definitions and collect the fields
for source in sources:
    for field in source["fields"]:
        if field not in all_fields:
            all_fields.append(field)

out = ds.CSVDataTarget("merged.csv")
out.fields = brewery.FieldList(all_fields)
out.initialize()

for source in sources:
    path = source["file"]

    # Initialize data source: skip reading of headers - we are preparing them ourselves
    # use XLSDataSource for XLS files
    # We ignore the fields in the header, because we have set-up fields
    # previously. We need to skip the header row.

    src = ds.CSVDataSource(path,read_header=False,skip_rows=1)
    src.fields = ds.FieldList(source["fields"])
    src.initialize()

    for record in src.records():

        # Add file reference into ouput - to know where the row comes from
        record["file"] = path
        out.append(record)

    # Close the source stream
    src.finalize()


