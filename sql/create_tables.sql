-- Create final table
IF OBJECT_ID('dbo.Sales', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Sales (
        OrderID INT PRIMARY KEY,
        OrderDate DATE,
        Region VARCHAR(50),
        CustomerID VARCHAR(50),
        Category VARCHAR(50),
        Product VARCHAR(100),
        Quantity INT,
        UnitPrice FLOAT,
        Sales FLOAT,
        Profit FLOAT
    );
END

-- Create logging table
IF OBJECT_ID('dbo.ETL_Log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ETL_Log (
        RunID INT IDENTITY(1,1) PRIMARY KEY,
        Run_Time DATETIME,
        Pipeline_Name VARCHAR(100),
        Status VARCHAR(20),
        Rows_Processed INT,
        Message VARCHAR(255)
    );
END