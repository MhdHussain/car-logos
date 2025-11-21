-- Create CarTypeMaster table in AdventureWorks2017 database
-- This table stores car makes and their associated logo paths

USE AdventureWorks2017;
GO

-- Create the CarTypeMaster table if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CarTypeMaster')
BEGIN
    CREATE TABLE dbo.CarTypeMaster
    (
        CarTypeID INT PRIMARY KEY IDENTITY(1,1),
        CarMake NVARCHAR(100) NOT NULL UNIQUE,
        LogoPath NVARCHAR(MAX) NULL,
        CreatedDate DATETIME2 DEFAULT GETUTCDATE(),
        ModifiedDate DATETIME2 DEFAULT GETUTCDATE()
    );

    -- Create an index on CarMake for faster lookups
    CREATE NONCLUSTERED INDEX IX_CarMake ON dbo.CarTypeMaster(CarMake);

    PRINT 'CarTypeMaster table created successfully.';
END
ELSE
BEGIN
    PRINT 'CarTypeMaster table already exists.';
END
GO
