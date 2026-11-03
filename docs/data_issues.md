# Data issues log

Issues found in the Olist data by the silver-layer checks (all 22 checks are logged to the `silver_dq_log` table by `notebooks/02_silver_clean.py`).

## Issues found

| Table | Issue | Count | Handling |
| --- | --- | --- | --- |
| orders | Status is not delivered | 2,963 | Kept; `is_delivered = false` |
| orders | Delivered but missing delivered_customer_date | 8 | Kept; `dq_missing_delivery_date = true` |
| orders | delivered_carrier_date before purchase | 166 | Kept; reviewed in analysis |
| products | Missing product_category_name | 610 | Category set to `unknown` |
| products | Categories without an English translation | 2 | Fall back to the Portuguese name |
| reviews | review_id appears more than once | 814 | Resolved by keeping one review per order |
| reviews | Orders with more than one review | 547 | Kept the latest review per order |
| payments | payment_value of 0 or less | 9 | Flagged only |
| geolocation | Exact duplicate rows | 261,831 | Collapsed to one row per zip prefix (19,015 rows) |

## Checks that found nothing (0 rows)

- Duplicate order_id, customer_id, seller_id, product_id
- Duplicate (order_id, order_item_id) rows and exact duplicate payment rows
- delivered_customer_date before purchase
- Orders whose customer_id is missing from customers
- Order items whose order_id or seller_id is missing from orders or sellers
- price of 0 or less or null; freight_value below 0 or null
- review_score outside 1 to 5 or null

## Other notes

- Bronze keeps every column as a string, exactly as received; types are cast in silver.
- The data ends in August 2018, so the last months are incomplete and their low late rates should not be read as improvement.
- Seller-customer distance is computed from zip-prefix centroids, so it is approximate.
