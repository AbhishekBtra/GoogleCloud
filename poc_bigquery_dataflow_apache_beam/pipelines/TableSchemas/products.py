class Products:
    "This class merely contains the schema for products table"
    schema = {
    "namespace": "products.avro",
    "type": "record",
    "name": "product",
    "fields": [
        {"name": "SKU"			    , "type": ["string",	"null"]},
        {"name": "name"				, "type": ["string",	"null"]},
        {"name": "orderedQuantiy"	, "type": ["int",	"null"]},
        {"name": "stockLevel"		, "type": ["int",	"null"]},
        {"name": "restockingLeadTime"	, "type": ["int",	"null"]},
        {"name": "sentimentScore"		, "type": ["float",	"null"]},
        {"name": "sentimentMagnitude"	, "type": ["float",	"null"]}
    ]
}
    bq_table_schema = """
                            SKU:STRING,
                            name:STRING,
                            orderedQuantiy:INTEGER,
                            stockLevel:INTEGER,
                            restockingLeadTime:INTEGER,
                            sentimentScore:FLOAT,
                            sentimentMagnitude:FLOAT
            """