create or refresh streaming live table customers_bronze
TBLPROPERTIES (
  'pipelines.autoCaptureSchema' = 'true',
  'pipelines.schemaInference.enabled' = 'true'
)
as 
select * from cloud_files('s3://e-commerce-daily-ingestion/customers/', 'json');

create or refresh streaming live table products_bronze
TBLPROPERTIES (
  'pipelines.autoCaptureSchema' = 'true',
  'pipelines.schemaInference.enabled' = 'true'
)
as 
select * from cloud_files('s3://e-commerce-daily-ingestion/products/');

create or refresh streaming live table orders_bronze
TBLPROPERTIES (
  'pipelines.autoCaptureSchema' = 'true',
  'pipelines.schemaInference.enabled' = 'true'
)
as 
select * from cloud_files('s3://e-commerce-daily-ingestion/orders/');

----------------------------------- SILVER LAYER ----------------------------------

create or refresh streaming live table valid_customers(
constraint valid_customers expect(customer_id is not null and email is not null and name is not null) on violation drop row)
as select *
from stream(live.customers_bronze);

create or refresh streaming live table valid_products(
constraint valid_products expect(product_id is not null and name is not null and price is not null) on violation drop row)
as select *
from stream(live.products_bronze);

create or refresh streaming live table valid_orders(
constraint valid_customers expect(customer_id is not null) on violation drop row,
constraint valid_products expect(product_id is not null) on violation drop row,
constraint valid_orders expect(order_id is not null and quantity is not null and total_price is not null) on violation drop row
)
as select *
from stream(live.orders_bronze);

--------------------- SILVER LAYER: SILVER_TABLE WITH SCD IMPLEMENTATION ----------------------

create or refresh streaming live table customers_silver
comment 'This table is a silver table with SCD type 1 implementation';

apply changes into customers_silver
from stream(live.valid_customers)
keys(customer_id)
sequence by timestamp
columns * except(_rescued_data)
stored as scd type 1;

create or refresh streaming live table products_silver
comment 'This table is a silver table with SCD type 2 implementation';

apply changes into products_silver
from stream(live.valid_products)
keys(product_id)
sequence by timestamp
columns * except(_rescued_data)
stored as scd type 2;

create or refresh streaming live table orders_silver
as
select * except(_rescued_data)
from stream(live.valid_orders);

-------------------------- GOLD TABLE: BUSINESS LOGIC IMPLEMENTATION --------------------------

create or refresh live table top_performing_products
comment 'Gold table of best selling products based on total revenue'
as
select p.product_id, 
p.name, p.category, sum(o.quantity) as total_units_sold, sum(total_price) as total_revenue
from live.orders_silver o
join live.products_silver p on o.product_id = p.product_id
group by p.product_id, p.name, p.category
order by total_revenue desc;

create or refresh live table customer_segments
comment 'Customer Segmentation based on total amount spend by customer'
SELECT 
  c.customer_id, 
  c.name, 
  SUM(o.total_price) AS total_spent, 
  COUNT(o.order_id) AS total_orders,
  CASE 
    WHEN SUM(o.total_price) > 20000 THEN 'VIP'
    WHEN SUM(o.total_price) > 5000 THEN 'Regular'
    ELSE 'New'
  END AS segment
FROM customers_silver c
JOIN orders_silver o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name;

create or refresh live table top_selling_month
as
select date_format(order_date, 'yyyy-MM') as sales_month, sum(total_price) as total_revenue
from orders_silver
group by date_format(order_date, 'yyyy-MM')
order by total_revenue desc;
