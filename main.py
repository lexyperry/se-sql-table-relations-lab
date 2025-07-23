# STEP 0

##Import libraries and connect to the SQLite database
import sqlite3
import pandas as pd

# Establish connection to the database file
conn = sqlite3.connect('data.sqlite')

# Preview all tables and schema info from the database
table = pd.read_sql("""SELECT * FROM sqlite_master""", conn)
print(table)

employee_table = pd.read_sql("""SELECT * FROM employees""", conn)
# STEP 1
df_boston = pd.read_sql("""SELECT * FROM offices""", conn)

## Get all employees in the Boston office
df_boston = pd.read_sql("""
SELECT firstName, jobTitle
FROM employees
JOIN offices
    ON employees.officeCode = offices.officeCode 
WHERE offices.city = "Boston"
""", conn)

print(df_boston)

# STEP 2 Identify offices with zero employees assigned
df_zero_emp =pd.read_sql("""
SELECT 
    offices.officeCode, 
    COUNT(employees.employeeNumber) AS number_employees
FROM offices
LEFT JOIN employees
    USING(officeCode)
GROUP BY offices.officeCode
HAVING number_employees = 0 
""", conn)

# STEP 3 List all employees with their associated office city and state
df_employee = pd.read_sql("""
SELECT employees.firstName, employees.lastName, offices.city, offices.state
FROM employees
    LEFT JOIN offices
    USING (officeCode)
ORDER BY employees.firstName, employees.lastName ASC
""", conn)

# STEP 4 Get customers who have not placed any orders, along with their contact info and sales rep
df_contacts = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers AS c
LEFT JOIN orders o
    USING(customerNumber)
WHERE o.customerNumber IS NULL
ORDER BY c.contactLastName ASC;
""", conn)

# STEP 5 Show all customer payments with contact names, sorted by amount (descending)
df_payment =  pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
FROM customers c
JOIN payments p ON c.customerNumber = p.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC;
""", conn)

# STEP 6 Identify employees whose customers have an average credit limit over 90,000, showing employee info and customer count
df_credit = pd.read_sql("""
SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS num_customers
FROM employees e
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber
HAVING AVG(c.creditLimit) > 90000
ORDER BY num_customers DESC
LIMIT 4;
""", conn)

# STEP 7 For each product, get total number of orders and total units sold
df_product_sold = pd.read_sql("""
SELECT p.productName, COUNT(od.orderNumber) AS numorders, SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
GROUP BY p.productCode
ORDER BY totalunits DESC;
""", conn)

# STEP 8 For each product, find the number of unique customers who have purchased it
df_total_customers = pd.read_sql("""
SELECT p.productName, p.productCode, COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
JOIN orders o ON od.orderNumber = o.orderNumber
GROUP BY p.productCode
ORDER BY numpurchasers DESC;
""", conn)

# STEP 9 Show number of customers per office 
df_customers = pd.read_sql("""
SELECT o.officeCode, o.city, COUNT(c.customerNumber) AS n_customers
FROM offices o
JOIN employees e ON o.officeCode = e.officeCode
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode;
""", conn)

# STEP 10  Identify employees who have sold products purchased by fewer than 20 customers
df_under_20 = pd.read_sql("""
SELECT DISTINCT e.employeeNumber, e.firstName, e.lastName, o.city, o.officeCode
FROM employees e
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders odr ON c.customerNumber = odr.customerNumber
JOIN orderdetails od ON odr.orderNumber = od.orderNumber
JOIN products p ON od.productCode = p.productCode
JOIN offices o ON e.officeCode = o.officeCode
WHERE p.productCode IN (
    SELECT od.productCode
    FROM orderdetails od
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY od.productCode
    HAVING COUNT(DISTINCT o.customerNumber) < 20
)
ORDER BY e.firstName = 'Loui' DESC, e.firstName;
""", conn)

# Close the database connection
conn.close()

