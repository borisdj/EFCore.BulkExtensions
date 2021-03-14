-- Clear Tables and Resets AutoIncrement
DELETE FROM [his].[ItemHistory];
DELETE FROM [dbo].[Item];
DBCC CHECKIDENT ('[dbo].[Item]', RESEED, 0);

-- CREATE TempTable -- instead of TOP 0, can use at end: 'WHERE 1 = 2'
SELECT TOP 0 T.[ItemId], T.[Description], T.[Name], T.[Price], T.[Quantity], T.[TimeUpdated] -- All columns: SELECT TOP 0 Source.*
INTO [dbo].[ItemTemp1234] FROM [dbo].[Item] AS T
LEFT JOIN [dbo].[Item] AS Source ON 1 = 0; -- removes Identity constrain
-- CREATE TempTableOutput
SELECT TOP 0 T.[ItemId], T.[Description], T.[Name], T.[Price], T.[Quantity], T.[TimeUpdated]
INTO [dbo].[ItemTemp1234Output] FROM [dbo].[Item] AS T
LEFT JOIN [dbo].[Item] AS Source ON 1 = 0;

-- INSERT INTO TempTable
INSERT INTO [dbo].[ItemTemp1234]
([ItemId], [Description], [Name], [Price], [Quantity], [TimeUpdated])
VALUES
(1, 'Desc1', 'SomeName1', 22.11, 34, '2020-01-01'),
(2, 'Desc2', 'SomeName2', 12.66, 45, '2020-01-01'),
(3, 'Desc3', 'SomeName3', 14.35, 46, '2020-01-01');

-- INSERT/UPDATE(DELETE) with MERGE from TempTable
MERGE [dbo].[Item] WITH (HOLDLOCK) AS T
USING (SELECT TOP 2147483647 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S
ON T.[ItemId] = S.[ItemId]
WHEN NOT MATCHED BY TARGET THEN INSERT ([Description], [Name], [Price], [Quantity], [TimeUpdated])
VALUES (S.[Description], S.[Name], S.[Price], S.[Quantity], S.[TimeUpdated])
WHEN MATCHED AND EXISTS (SELECT S.[Description], S.[Name], S.[Price], S.[Quantity], S.[TimeUpdated]
EXCEPT SELECT T.[Description], T.[Name], T.[Price], T.[Quantity], T.[TimeUpdated])
THEN UPDATE SET T.[Description] = S.[Description], T.[Name] = S.[Name], T.[Price] = S.[Price], T.[Quantity] = S.[Quantity], T.[TimeUpdated] = S.[TimeUpdated]
--WHEN NOT MATCHED BY SOURCE THEN DELETE
--OUTPUT COALESCE(INSERTED.[ItemId], DELETED.[ItemId]), COALESCE(INSERTED.[Description], DELETED.[Description]), COALESCE(INSERTED.[Name], DELETED.[Name]), COALESCE(INSERTED.[Price], DELETED.[Price]), COALESCE(INSERTED.[Quantity], DELETED.[Quantity]), COALESCE(INSERTED.[TimeUpdated], DELETED.[TimeUpdated])
OUTPUT INSERTED.[ItemId], INSERTED.[Description], INSERTED.[Name], INSERTED.[Price], INSERTED.[Quantity], INSERTED.[TimeUpdated] -- All columns: INSERTED.*
INTO [dbo].[ItemTemp1234Output];

-- INSERT Item without corresponding value in ItemTemp1234, so it will be deleted when enabling DELETE in the previous query
INSERT INTO [dbo].[Item]
([Description], [Name], [Price], [Quantity], [TimeUpdated])
VALUES
('Desc4', 'SomeName4', 16.12, 39, '2020-01-01')

-- Delete TempTable
DROP TABLE [dbo].[ItemTemp1234];
DROP TABLE [dbo].[ItemTemp1234Output];

-- EXAMPLE 2.
-- INSERT/UPDATE when having CompositeKey,
MERGE [dbo].[UserRole] WITH (HOLDLOCK) AS T
USING (SELECT TOP 2147483647 * FROM [dbo].[UserRoleTemp1234] ORDER BY [UserId], [RoleId]) AS S
ON T.[UserId] = S.[UserId] AND T.[RoleId] = S.[RoleId]
-- ON (T.[UserId] = S.[UserId] OR (T.[UserId] IS NULL AND S.[UserId] IS NULL)) AND (T.[RoleId] = S.[RoleId] OR (T.[RoleId] IS NULL AND S.[RoleId] IS NULL)) -- when are Nullable
WHEN NOT MATCHED BY TARGET THEN INSERT ([Description])
VALUES (S.[Description])
WHEN MATCHED THEN UPDATE SET T.[Description] = S.[Description];

----------BATCH Examples----------

-- BatchDelete
SELECT [a].[ItemId], [a].[Description], [a].[Name], [a].[Price], [a].[Quantity], [a].[TimeUpdated]\r\nFROM [Item] AS [a]
DELETE [a]																							  FROM [Item] AS [a]
DELETE																								  FROM [Item]

-- BatchDelete Where
SELECT [a].[ItemId], [a].[Description], [a].[Name], [a].[Price], [a].[Quantity], [a].[TimeUpdated]    FROM [Item] AS [a]    WHERE [a].[ItemId] > 500
DELETE [a]																							  FROM [Item] AS [a]	WHERE [a].[ItemId] > 500
DELETE																								  FROM [Item]			WHERE [ItemId] > 500

-- BatchUpdate Where
SELECT [a].[ItemId], [a].[Description], [a].[Name], [a].[Price], [a].[Quantity], [a].[TimeUpdated]    FROM [Item] AS [a]    WHERE ([a].[ItemId] <= 500) AND ([a].[Price] >= 0.0)
UPDATE [a] SET [Description] = N'Updated', [Price] = '1.5'											  FROM [Item] AS [a]	WHERE ([a].[ItemId] <= 500) AND ([a].[Price] >= 0.0)
UPDATE [Item] SET [Description] = N'Updated', [Price] = '1.5'																WHERE ([ItemId] <= 500) AND ([Price] >= 0.0)