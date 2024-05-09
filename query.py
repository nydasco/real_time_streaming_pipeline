import polars as pl

# Configuration
delta_path = "./delta_tables"

# Create DataFrames
client = pl.read_delta(f"{delta_path}/client")
department = pl.read_delta(f"{delta_path}/department")
employee = pl.read_delta(f"{delta_path}/employee")
sale = pl.read_delta(f"{delta_path}/sale")

# Add Columns in DataFrames
client = client.with_columns(
    pl.concat_str(["first_name", "last_name"], separator = " ").alias("client_name")
    )

employee = employee.with_columns(
    pl.concat_str(["first_name", "last_name"], separator = " ").alias("employee_name")
    )

# Join DataFrames
employee_dept = employee.join(
        department, 
        left_on = "department_id", 
        right_on = "id", 
        how = "inner").select(
                ["id", "employee_name", "department_id", "department"]
                )

print(employee_dept)

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
                        ["id", "employee_name", "department", "client_name", "date", "region", "sale"]
                        )

print(complete)
