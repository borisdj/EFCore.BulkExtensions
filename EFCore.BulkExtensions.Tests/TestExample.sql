-- Clear Tables and Resets AutoIncrement
DELETE FROM his.[ItemHistory];
DELETE FROM dbo.[Item];
DBCC CHECKIDENT ('dbo.[Item]', RESEED, 0);

-- Create TempTable
SELECT TOP 0 Source.* INTO dbo.[ItemTemp1234] FROM dbo.[Item]
LEFT JOIN dbo.[Item] AS Source ON 1 = 0;
-- Create TempTableOutput
SELECT TOP 0 Source.* INTO dbo.[ItemTemp1234Output] FROM dbo.[Item]
LEFT JOIN dbo.[Item] AS Source ON 1 = 0;

-- Insert into TempTable
INSERT INTO dbo.[ItemTemp1234]
([ItemId], [Description], [Name], [Price], [Quantity], [TimeUpdated])
Values
(1, 'Desc1', 'SomeName1', 22.11, 34, '2017-01-01'),
(2, 'Desc2', 'SomeName2', 12.66, 45, '2017-01-01'),
(3, 'Desc3', 'SomeName3', 14.35, 46, '2017-01-01');

-- Insert/Update with MERGE from TempTable
MERGE dbo.[Item] WITH (HOLDLOCK) AS T USING (SELECT TOP 2147483647 * FROM dbo.[ItemTemp1234] ORDER BY [ItemId]) AS S
ON T.[ItemId] = S.[ItemId]
WHEN NOT MATCHED THEN INSERT
([Description], [Name], [Price], [Quantity], [TimeUpdated])
VALUES
(S.[Description], S.[Name], S.[Price], S.[Quantity], S.[TimeUpdated])
WHEN MATCHED THEN UPDATE SET
T.[Description] = S.[Description],
T.[Name] = S.[Name],
T.[Price] = S.[Price],
T.[Quantity] = S.[Quantity],
T.[TimeUpdated] = S.[TimeUpdated]
OUTPUT inserted.* INTO dbo.[ItemTemp1234Output];

-- Insert/Update when having CompositeKey
MERGE dbo.[UserRole] WITH (HOLDLOCK) AS T USING (SELECT TOP 2147483647 * FROM dbo.[UserRoleTemp1234] ORDER BY [UserId], [RoleId]) AS S
ON T.[UserId] = S.[UserId] AND T.[RoleId] = S.[RoleId]
WHEN NOT MATCHED THEN INSERT
([Description])
VALUES
(S.[Description])
WHEN MATCHED THEN UPDATE SET
T.[Description] = S.[Description];

-- Delete TempTable
DROP TABLE dbo.[ItemTemp1234];
DROP TABLE dbo.[ItemTemp1234Output];