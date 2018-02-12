-- Clear Tables and Resets AutoIncrement
DELETE FROM his.[ItemHistory];
DELETE FROM dbo.[Item];
DBCC CHECKIDENT ('dbo.[Item]', RESEED, 0);

-- CREATE TempTable -- instead of TOP 0, can use at end: 'WHERE 1 = 2'
SELECT TOP 0 T.[ItemId], T.[Description], T.[Name], T.[Price], T.[Quantity], T.[TimeUpdated] -- All columns: SELECT TOP 0 Source.*
INTO dbo.[ItemTemp1234] FROM dbo.[Item] AS T
LEFT JOIN dbo.[Item] AS Source ON 1 = 0; -- removes Identity constrain
-- CREATE TempTableOutput
SELECT TOP 0 T.[ItemId], T.[Description], T.[Name], T.[Price], T.[Quantity], T.[TimeUpdated]
INTO dbo.[ItemTemp1234Output] FROM dbo.[Item] AS T
LEFT JOIN dbo.[Item] AS Source ON 1 = 0;

-- INSERT INTO TempTable
INSERT INTO dbo.[ItemTemp1234]
([ItemId], [Description], [Name], [Price], [Quantity], [TimeUpdated])
VALUES
(1, 'Desc1', 'SomeName1', 22.11, 34, '2017-01-01'),
(2, 'Desc2', 'SomeName2', 12.66, 45, '2017-01-01'),
(3, 'Desc3', 'SomeName3', 14.35, 46, '2017-01-01');

-- INSERT/UPDATE with MERGE from TempTable
MERGE dbo.[Item] WITH (HOLDLOCK) AS T
USING (SELECT TOP 2147483647 * FROM dbo.[ItemTemp1234] ORDER BY [ItemId]) AS S
ON T.[ItemId] = S.[ItemId]
WHEN NOT MATCHED THEN INSERT ([Description], [Name], [Price], [Quantity], [TimeUpdated])
VALUES (S.[Description], S.[Name], S.[Price], S.[Quantity], S.[TimeUpdated])
WHEN MATCHED THEN UPDATE SET T.[Description] = S.[Description], T.[Name] = S.[Name], T.[Price] = S.[Price], T.[Quantity] = S.[Quantity], T.[TimeUpdated] = S.[TimeUpdated]
OUTPUT INSERTED.[ItemId], INSERTED.[Description], INSERTED.[Name], INSERTED.[Price], INSERTED.[Quantity], INSERTED.[TimeUpdated] -- All columns: INSERTED.*
INTO dbo.[ItemTemp1234Output];

-- INSERT/UPDATE when having CompositeKey
MERGE dbo.[UserRole] WITH (HOLDLOCK) AS T
USING (SELECT TOP 2147483647 * FROM dbo.[UserRoleTemp1234] ORDER BY [UserId], [RoleId]) AS S
ON T.[UserId] = S.[UserId] AND T.[RoleId] = S.[RoleId]
WHEN NOT MATCHED THEN INSERT ([Description])
VALUES (S.[Description])
WHEN MATCHED THEN UPDATE SET T.[Description] = S.[Description];

-- Delete TempTable
DROP TABLE dbo.[ItemTemp1234];
DROP TABLE dbo.[ItemTemp1234Output];