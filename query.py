#!/usr/bin/env python3

import polars as pl

# Configuration
delta_path = "./delta_tables"

# Create DataFrames for each delta table
client = pl.read_delta(f"{delta_path}/client")
department = pl.read_delta(f"{delta_path}/department")
employee = pl.read_delta(f"{delta_path}/employee")
sale = pl.read_delta(f"{delta_path}/sale")

# Add Columns in DataFrames, specifically adding the concatenation of first and last names with a space separator
client = client.with_columns(
    pl.concat_str(["first_name", "last_name"], separator = " ").alias("client_name")
    )

employee = employee.with_columns(
    pl.concat_str(["first_name", "last_name"], separator = " ").alias("employee_name")
    )

# Join DataFrames, initially the employee and department DataFrames, followed by the sale DataFrame with the 
# previously joined DataFrame plus the client DataFrame. We then select the fields we want to use and change the 
# datatypes of those that we need to.
employee_dept = employee.join(
        department, 
        left_on = "department_id", 
        right_on = "id", 
        how = "inner").select(
                ["id", "employee_name", "department_id", "department"]
                )

complete = sale.join(
        employee_dept,
        left_on = "employee_id", 
        right_on = "id", 
        ).join(
                client, 
                left_on = "client_id", 
                right_on = "id", 
                how = "inner"
                ).select(
                        pl.col("id").str.to_integer(),
                        pl.col("employee_name"), 
                        pl.col("department"), 
                        pl.col("client_name"), 
                        pl.col("date").str.to_date("%d/%m/%Y"), 
                        pl.col("region"), 
                        pl.col("sale").str.to_decimal(2),
                        )

print(complete)
