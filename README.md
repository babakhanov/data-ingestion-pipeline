# Data Ingestion Pipeline for E-Commerce platform

## Overview

This repository contains a data ingestion pipeline designed to handle two datasets from an e-commerce platform: Orders and Inventories. The pipeline processes raw data, performs transformations, and combines both datasets into a unified schema for storage and analysis.

### Features

- Data ingestion for orders and inventory
- Automated data validation and cleaning
- Scalable architecture using Docker containers
- Managed workflow orchestration with Prefect

## Setup instructions

Clone repository:
```
git clone git@github.com:babakhanov/data-ingestion-pipeline.git
cd data-ingestion-pipeline
```
Launch the pipeline:
```
docker compose up
```

### Database Access
Connect to PostgreSQL console:
```
docker exec -it injest-data-app-db-1 psql -U postgres -d data_app
```

### Prefect dashboard access

Open in your browser http://0.0.0.0:4200 to get to the prefect dashboard

## Report queries

### Total Revenue per Product

This report aggregates the total revenue generated per product. The total revenue can be calculated by multiplying the `quantity` of each order by its corresponding `amount`.

```
SELECT
    o.product_id,
    i.name AS product_name,
    SUM(o.quantity * o.amount) AS total_revenue
FROM
    orders o
JOIN
    inventories i ON o.product_id = i.product_id
GROUP BY
    o.product_id, i.name
ORDER BY
    total_revenue DESC;
```

###  Low Stock Report (Inventory Below Threshold)
This report identifies products that have inventory levels below a certain threshold (e.g., 10 units). This helps e-commerce managers quickly identify products that need restocking.

```
SELECT
    i.product_id,
    i.name AS product_name,
    i.quantity AS current_stock,
    i.category,
    i.sub_category
FROM
    inventories i
WHERE
    i.quantity < 10
ORDER BY
    i.quantity ASC;
```

### Total Orders per Product per Month
This report shows how many orders were placed for each product during each month. It helps track trends over time and identify seasonal products.

```
SELECT
    o.product_id,
    i.name AS product_name,
    EXTRACT(MONTH FROM o.date_time) AS month,
    EXTRACT(YEAR FROM o.date_time) AS year,
    SUM(o.quantity) AS total_orders
FROM
    orders o
JOIN
    inventories i ON o.product_id = i.product_id
GROUP BY
    o.product_id, i.name, EXTRACT(MONTH FROM o.date_time), EXTRACT(YEAR FROM o.date_time)
ORDER BY
    year DESC, month DESC, total_orders DESC;
```

### Total Revenue per Category
This report aggregates revenue per product category. It can help understand which product categories are generating the most revenue.

```
SELECT
    i.category,
    SUM(o.quantity * o.amount) AS total_revenue
FROM
    orders o
JOIN
    inventories i ON o.product_id = i.product_id
GROUP BY
    i.category
ORDER BY
    total_revenue DESC;
```

### Inventory Status for a Specific Product
This report allows checking the current stock and sales status for a particular product. This is useful for monitoring a single product's performance.

```
SELECT
    i.product_id,
    i.name AS product_name,
    i.quantity AS current_stock,
    SUM(o.quantity) AS total_sold,
    (i.quantity - SUM(o.quantity)) AS remaining_stock
FROM
    inventories i
LEFT JOIN
    orders o ON i.product_id = o.product_id
WHERE
    i.product_id = 'prod1548'  -- Replace with the actual product ID you want to query
GROUP BY
    i.product_id, i.name, i.quantity
```

### Most Sold Products by Category
This report identifies the best-selling products in each category. It helps identify which products are performing best within specific categories.

```
SELECT
    i.category,
    i.product_id,
    i.name AS product_name,
    SUM(o.quantity) AS total_sold
FROM
    orders o
JOIN
    inventories i ON o.product_id = i.product_id
GROUP BY
    i.category, i.product_id, i.name
ORDER BY
    total_sold DESC;
```

   

## Conclusion
These report queries cover a wide range of business needs such as:

- **Revenue analysis** (total revenue per product, per category).
- **Stock monitoring** (low stock products, remaining stock).
- **Sales performance** (order volume by channel, best-selling products).
- **Inventory health** (inventory status for a specific product).

## Possible Improvements

1. **Automated Scheduling**
   - Implement Prefect task scheduling for automated data ingestion
   - Add retry mechanisms for failed tasks

2. **Data Quality**
   - Implement data quality checks
   - Create alerts for data anomalies

3. **Performance Optimization**
   - Add data partitioning for large datasets
   - Implement parallel processing for data transformation
   - Optimize database indices and query performance

4. **Monitoring and Observability**
   - Add comprehensive logging
   - Implement metrics collection
   - Create performance dashboards
   - Set up alerting for pipeline failures

5. **Infrastructure**
   - Implement infrastructure as code using Terraform
   - Add environment-specific configurations
   - Set up CI/CD pipeline for automated deployments

6. **Web Application Dashboard**
   - Build a web interface for common reports and analytics:
     - Sales performance dashboard
     - Inventory status overview
     - Real-time stock alerts
     - Product performance metrics
     - Custom report builder
   - Implement user authentication
   - Add export functionality for reports (CSV, Excel)
   - Enable custom date range selection for all reports
   - Add interactive charts and visualizations using libraries like Chart.js or D3.js
