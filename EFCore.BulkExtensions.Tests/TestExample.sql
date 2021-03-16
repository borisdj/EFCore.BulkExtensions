-- Clear Tables and Resets AutoIncrement
DELETE FROM [his].[ItemHistory];
DELETE FROM [dbo].[Item];
DBCC CHECKIDENT ('[dbo].[Item]', RESEED, 0);

-- CREATE TempTable -- instead of TOP 0, can use at end: 'WHERE 1 = 2'
SELECT TOP 0 T.[ItemId], T.[Name], T.[Description], T.[Quantity], T.[Price], T.[TimeUpdated] -- All columns: SELECT TOP 0 Source.*
INTO [dbo].[ItemTemp1234] FROM [dbo].[Item] AS T
LEFT JOIN [dbo].[Item] AS Source ON 1 = 0; -- removes Identity constrain
-- CREATE TempTableOutput
SELECT TOP 0 T.[ItemId], T.[Name], T.[Description], T.[Quantity], T.[Price], T.[TimeUpdated]
INTO [dbo].[ItemTemp1234Output] FROM [dbo].[Item] AS T
LEFT JOIN [dbo].[Item] AS Source ON 1 = 0;

-- INSERT INTO TempTable
INSERT INTO [dbo].[ItemTemp1234]
([ItemId], [Name], [Description], [Quantity], [Price], [TimeUpdated])
VALUES
(1, 'SomeName1', 'Desc1', 34, 22.11, '2020-01-01'),
(2, 'SomeName2', 'Desc2', 45, 12.66, '2020-01-01'),
(3, 'SomeName3', 'Desc3', 46, 14.35, '2020-01-01');

-- INSERT/UPDATE(DELETE) with MERGE from TempTable
MERGE [dbo].[Item] WITH (HOLDLOCK) AS T
USING (SELECT TOP 2147483647 * FROM [dbo].[ItemTemp1234] ORDER BY [ItemId]) AS S
ON T.[ItemId] = S.[ItemId]
WHEN NOT MATCHED BY TARGET THEN INSERT ([Name], [Description], [Quantity], [Price], [TimeUpdated])
VALUES (S.[Name], S.[Description], S.[Quantity], S.[Price], S.[TimeUpdated])
WHEN MATCHED AND EXISTS (SELECT S.[Name], S.[Description], S.[Quantity], S.[Price], S.[TimeUpdated]
EXCEPT SELECT T.[Name], T.[Description], T.[Quantity], T.[Price], T.[TimeUpdated])
THEN UPDATE SET T.[Name] = S.[Name], T.[Description] = S.[Description], T.[Quantity] = S.[Quantity], T.[Price] = S.[Price], T.[TimeUpdated] = S.[TimeUpdated]
--WHEN NOT MATCHED BY SOURCE THEN DELETE
--OUTPUT COALESCE(INSERTED.[ItemId], DELETED.[ItemId]), COALESCE(INSERTED.[Name], DELETED.[Name]), COALESCE(INSERTED.[Description], DELETED.[Description]), COALESCE(INSERTED.[Quantity], DELETED.[Quantity]), COALESCE(INSERTED.[Price], DELETED.[Price]), COALESCE(INSERTED.[TimeUpdated], DELETED.[TimeUpdated])
OUTPUT INSERTED.[ItemId], INSERTED.[Name], INSERTED.[Description], INSERTED.[Quantity], INSERTED.[Price], INSERTED.[TimeUpdated] -- All columns: INSERTED.*
INTO [dbo].[ItemTemp1234Output];

-- INSERT Item without corresponding value in ItemTemp1234, so it will be deleted when enabling DELETE in the previous query
INSERT INTO [dbo].[Item]
([Name], [Description], [Quantity], [Price], [TimeUpdated])
VALUES
('SomeName4', 'Desc4', 39, 16.12, '2020-01-01')

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
SELECT [a].[ItemId], [a].[Name], [a].[Description], [a].[Quantity], [a].[Price], [a].[TimeUpdated]    FROM [Item] AS [a]
DELETE [a]																							  FROM [Item] AS [a]
DELETE																								  FROM [Item]

-- BatchDelete Where
SELECT [a].[ItemId], [a].[Name], [a].[Description], [a].[Quantity], [a].[Price], [a].[TimeUpdated]    FROM [Item] AS [a]    WHERE [a].[ItemId] > 500
DELETE [a]																							  FROM [Item] AS [a]	WHERE [a].[ItemId] > 500
DELETE																								  FROM [Item]			WHERE [ItemId] > 500

-- BatchUpdate Where
SELECT [a].[ItemId], [a].[Name], [a].[Description], [a].[Quantity], [a].[Price], [a].[TimeUpdated]    FROM [Item] AS [a]    WHERE ([a].[ItemId] <= 500) AND ([a].[Price] >= 0.0)
UPDATE [a] SET [Description] = N'Updated', [Price] = '1.5'											  FROM [Item] AS [a]	WHERE ([a].[ItemId] <= 500) AND ([a].[Price] >= 0.0)
UPDATE [Item] SET [Description] = N'Updated', [Price] = '1.5'																WHERE ([ItemId] <= 500) AND ([Price] >= 0.0)