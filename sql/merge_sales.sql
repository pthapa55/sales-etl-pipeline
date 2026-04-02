MERGE dbo.Sales AS target
USING dbo.Sales_Staging AS source
ON target.OrderID = source.OrderID

WHEN MATCHED THEN
    UPDATE SET
        target.OrderDate = source.OrderDate,
        target.Region = source.Region,
        target.CustomerID = source.CustomerID,
        target.Category = source.Category,
        target.Product = source.Product,
        target.Quantity = source.Quantity,
        target.UnitPrice = source.UnitPrice,
        target.Sales = source.Sales,
        target.Profit = source.Profit

WHEN NOT MATCHED THEN
    INSERT (
        OrderID, OrderDate, Region, CustomerID, Category,
        Product, Quantity, UnitPrice, Sales, Profit
    )
    VALUES (
        source.OrderID, source.OrderDate, source.Region, source.CustomerID, source.Category,
        source.Product, source.Quantity, source.UnitPrice, source.Sales, source.Profit
    );