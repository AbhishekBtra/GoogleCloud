class Transactions:
    schema = {
                 "namespace": "rev_transactions.avro",
                 "type": "record",
                 "name": "rev_transactions",
                 "fields":
                     [
                         {"name": "channelGrouping", "type": ["string", "null"]},
                         {"name": "hits_time", "type": ["long", "null"]},
                         {"name": "geoNetwork_country", "type": ["string", "null"]},
                         {"name": "geoNetwork_city", "type": ["string", "null"]},
                         {"name": "totals_totalTransactionRevenue", "type": ["long", "null"]},
                         {"name": "totals_transactions", "type": ["long", "null"]},
                         {"name": "totals_timeOnSite", "type": ["long", "null"]},
                         {"name": "totals_pageviews", "type": ["long", "null"]},
                         {"name": "date", "type": ["string", "null"]},
                         {"name": "visitId", "type": ["long", "null"]},
                         {"name": "hits_type", "type": ["string", "null"]},
                         {"name": "hits_product_productRefundAmount", "type": ["long", "null"]},
                         {"name": "hits_product_productQuantity", "type": ["long", "null"]},
                         {"name": "hits_product_productPrice", "type": ["long", "null"]},
                         {"name": "hits_product_productRevenue", "type": ["long", "null"]},
                         {"name": "hits_product_productSKU", "type": ["string", "null"]},
                         {"name": "hits_product_v2ProductName", "type": ["string", "null"]},
                         {"name": "hits_product_v2ProductCategory", "type": ["string", "null"]},
                         {"name": "hits_product_productVariant", "type": ["string", "null"]},
                         {"name": "hits_item_currencyCode", "type": ["string", "null"]},
                         {"name": "hits_item_itemQuantity", "type": ["long", "null"]},
                         {"name": "hits_item_itemRevenue", "type": ["long", "null"]},
                         {"name": "hits_transaction_transactionRevenue", "type": ["long", "null"]},
                         {"name": "hits_transaction_transactionId", "type": ["string", "null"]},
                         {"name": "hits_page_pageTitle", "type": ["string", "null"]},
                         {"name": "hits_page_searchKeyword", "type": ["string", "null"]},
                         {"name": "hits_page_pagePathLevel1", "type": ["string", "null"]},
                     ]
             }