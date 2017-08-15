using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Dapper;
using My.Spruce.Extensions;
using My.Spruce.Schema.Attributes;

namespace My.Spruce.Schema
{
	/// <summary>
	/// These extensions are only really useful in the data namespace, so they are separate from the other DbConnectionExtensions.
	/// </summary>
	/// <remarks></remarks>
	public static class DbExtensions
	{
		#region Tables

		/// <summary>
		/// Check if the specified table exists
		/// Converted to MySql Format
		/// </summary>
		/// <param name="db">Database Connection</param>
		/// <param name="name">Name of the table</param>
		/// <param name="transaction">Database transaction</param>
		/// <returns></returns>
		public static bool TableExists(this IDbConnection db, string name, IDbTransaction transaction = null)
		{
			string schemaName = null;

			name = name.Replace("[", "");
			name = name.Replace("]", "");

			if (name.Contains("."))
			{
				var parts = name.Split('.');
				if (parts.Count() == 2)
				{
					schemaName = parts[0];
					name = parts[1];
				}
			}

			var builder = new StringBuilder("select * from INFORMATION_SCHEMA.TABLES where ");
			if (!String.IsNullOrEmpty(schemaName))
				builder.Append("TABLE_SCHEMA = @schemaName AND ");
			builder.Append("TABLE_NAME = @name LIMIT 1;");

            return db.Query(builder.ToString(), new { schemaName, name }, transaction: transaction).Count() == 1;
		}

		/// <summary>
		/// Creates the sql table for the specified type
		/// </summary>
		/// <param name="db">Database Connection</param>
		/// <typeparam name="T">Type representing the table to create</typeparam>
		public static void CreateTable<T>(this IDbConnection db)
		{
			db.CreateTable(typeof(T));
		}

	    /// <summary>
	    /// Creates the sql table for the specified type
	    /// </summary>
	    /// <param name="db">Database Connection</param>
	    /// <param name="type">Type representing the table to create</param>
	    public static void CreateTable(this IDbConnection db, Type type)
	    {
	        string tableName = db.GetTableName(type);

	        StringBuilder text = new StringBuilder();

	        StringBuilder constraints = new StringBuilder();

	        text.AppendLine(string.Format("CREATE TABLE IF NOT EXISTS `{0}` (", tableName));

	        var columns = db.GetColumns(type);
	        var columnIndex = 0;
	        foreach (var column in columns)
	        {
	            var isFirst = columnIndex++ == 0;
	            if (!isFirst)
	            {
	                text.AppendLine(",");
	            }

	            text.AppendFormat("`{0}` {1}", column.Name, column.SqlType);
	            if (column.IsPrimary)
	            {
	                text.Append(" PRIMARY KEY"); //not sure bout this one if its ok here or not
	                if (column.AutoIncrement)
	                {
	                    text.Append(" AUTO_INCREMENT");
	                }
	            }
	            else
	            {
	                if (column.IsNullable)
	                {
	                    text.Append(" NULL");
	                }
	                else
	                {
	                    text.Append(" NOT NULL");
	                }
	            }
	            if (!string.IsNullOrEmpty(column.DefaultValue))
	            {
	                text.AppendFormat(" DEFAULT ({0})", column.DefaultValue);
	            }

	            if (column.HasForeignKey && column.GenerateForeignKey)
	            {
	                constraints.Append(", CONSTRAINT \"");
	                constraints.Append(column.ForeignKeyName);
	                constraints.Append("\" FOREIGN KEY (\"");
	                constraints.Append(column.Name);
	                constraints.Append("\") REFERENCES \"");
	                constraints.Append(column.ReferencedTableName);
	                constraints.AppendLine("\" (\"Id\")");
	            }
	        }
	        if (constraints.Length > 0)
	            text.Append(constraints);

	        text.AppendLine(");");
//		        text.AppendLine("END;");

	        db.Execute(text.ToString());
	    }

	    /// <summary>
		/// Drops the specified table
		/// </summary>
		/// <typeparam name="T">Type representing the table to drop</typeparam>
		/// <param name="db">Database Connection</param>
		public static void DropTable<T>(this IDbConnection db)
		{
			db.DropTable(db.GetTableName<T>());
		}
		/// <summary>
		/// Drops the specified table
		/// </summary>
		/// <param name="db">Database Connection</param>
		/// <param name="name">Name fo the table to drop</param>
		public static void DropTable(this IDbConnection db, string name)
		{
		    if (db.TableExists(name))
		    {
		        db.Execute(string.Format("DROP TABLE {0}", name));
		    }
//			db.Execute("IF EXISTS(SELECT 1 FROM sys.objects WHERE OBJECT_ID = OBJECT_ID(N'{0}') AND type = (N'U')) DROP TABLE [{0}]".Fmt(name));
		}
		/// <summary>
		/// Drops all tables
		/// </summary>
		/// <param name="db">Database connection</param>
		public static void DropAllTables(this IDbConnection db)
		{
			db.Execute(
@"
DECLARE @SQL NVARCHAR(MAX)	
 
SELECT
	@SQL = '-- Start '
PRINT 'dropping all foreign keys...'

WHILE @SQL > ''
BEGIN
	SELECT
		@SQL = ''
 
	SELECT
		@SQL = @SQL + 
			CASE
				WHEN DATALENGTH(@SQL) < 7500 THEN
					N'alter table ' + QUOTENAME(schema_name(schema_id)) 
					+ N'.' 
					+ QUOTENAME(OBJECT_NAME(parent_object_id)) 
					+ N' drop constraint ' 
					+ name
					+ CHAR(13) + CHAR(10) --+ 'GO' + CHAR(13) + CHAR(10)
				ELSE
					''
			END
	FROM
		sys.foreign_keys
 
	PRINT @SQL
	EXEC SP_EXECUTESQL @SQL
	PRINT '---8<------------------------------------------------------------------------------------------'
END
 
---8<-------------------------------------------------------------
PRINT 'dropping all tables...'
 
exec sp_msforeachtable 'drop table ?'

PRINT 'all tables dropped.'
PRINT 'All done'");
		}

		/// <summary>
		/// Rename the specified table
		/// </summary>
		/// <typeparam name="T">Type representing the new table name</typeparam>
		/// <param name="db">Database connection</param>
		/// <param name="oldTableName">Old table name</param>
		public static void RenameTable<T>(this IDbConnection db, string oldTableName)
		{
			db.RenameTable(oldTableName, typeof(T));
		}
		/// <summary>
		/// Rename the specified table
		/// </summary>
		/// <param name="db">Database connection</param>
		/// <param name="oldTableName">Old table name</param>
		/// <param name="tableType">Type representing the new table name</param>
		public static void RenameTable(this IDbConnection db, string oldTableName, Type tableType)
		{
			db.RenameTable(oldTableName, db.GetTableName(tableType));
		}
		/// <summary>
		/// Rename the specified table
		/// </summary>
		/// <param name="db">Database connection</param>
		/// <param name="oldTableName">Old table name</param>
		/// <param name="newTableName">New table name</param>
		public static void RenameTable(this IDbConnection db, string oldTableName, string newTableName)
		{
			if (newTableName.Equals(oldTableName))
				return;

			db.Execute(@"
	RENAME TABLE @OldTableName TO @NewTableName", new
				{
					oldTableName,
					newTableName
				});
		}

		private static readonly ConcurrentDictionary<RuntimeTypeHandle, IList<Column>> Columns = new ConcurrentDictionary<RuntimeTypeHandle, IList<Column>>();
		/// <summary>
		/// Gets the column definitions for the specified table
		/// </summary>
		/// <typeparam name="T">Type representing the table</typeparam>
		/// <param name="db">Database connection</param>
		/// <returns></returns>
		public static IList<Column> GetColumns<T>(this IDbConnection db)
		{
			return db.GetColumns(typeof(T));
		}
		/// <summary>
		/// Gets the column definitions for the specified table
		/// </summary>
		/// <param name="db">Database connection</param>
		/// <param name="type">Type representing the table</param>
		/// <returns></returns>
		public static IList<Column> GetColumns(this IDbConnection db, Type type)
		{
			IList<Column> columns;
			if (!Columns.TryGetValue(type.TypeHandle, out columns))
			{
				columns = new List<Column>();
				var columnIndex = 0;
				var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance).ToList();
				var hasIdField = properties.Any(x => x.Name.Equals("Id", StringComparison.OrdinalIgnoreCase));
				string tableName = null;
				foreach (var property in properties)
				{
					var ignore = property.FirstAttribute<IgnoreAttribute>();
					if (ignore != null)
						continue;

					// Only map properties with get and set accessors
					if (!property.CanWrite || !property.CanRead)
						continue;

					var isFirst = columnIndex++ == 0;
					var pkAttribute = property.FirstAttribute<PrimaryKeyAttribute>();
					var isPrimary = (!hasIdField && isFirst) || pkAttribute != null || property.Name.Equals("Id", StringComparison.OrdinalIgnoreCase);
					var isNullableType = property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>);
					var isNullable = (!property.PropertyType.IsValueType && property.FirstAttribute<RequiredAttribute>() == null) || isNullableType;
					var propertyType = isNullableType
						? Nullable.GetUnderlyingType(property.PropertyType)
						: property.PropertyType;
					var sqlType = GetSqlType(propertyType, property);
					var autoIncrement = isPrimary && property.FirstAttribute<AutoIncrementAttribute>() != null;
					var defaultValueAttribute = property.FirstAttribute<DefaultAttribute>();
					var defaultValue = defaultValueAttribute != null ? defaultValueAttribute.DefaultValue : null;
					//NOTE: This as dynamic trick should be able to handle column attributes from different places
					var columnAttr = property.GetCustomAttributes(false).SingleOrDefault(attr => attr.GetType().Name == "ColumnAttribute") as dynamic;

					var closureProperty = property;
					var column = new Column
						{
							IsPrimary = isPrimary,
							IsNullable = isNullable,
							Type = propertyType,
							Name = columnAttr == null ? property.Name : columnAttr.Name,
							PropertyName = property.Name,
							SqlType = sqlType,
							AutoIncrement = autoIncrement,
							DefaultValue = defaultValue,
							SetValue = closureProperty.SetValue,
							GetValue = closureProperty.GetValue
						};

					var referencesAttribute = property.FirstAttribute<ReferencesAttribute>();
					if (referencesAttribute != null)
					{
						if (tableName == null)
							tableName = db.GetTableName(type);

						var referencedClassType = referencesAttribute.Type;
						column.HasForeignKey = true;
						// TODO: Separate out logic for getting primary columns. The code below will infinite loop if two tables reference each other.
						if (referencedClassType == type)
						{
							var id = columns.SingleOrDefault(x => x.IsPrimary);
							column.ReferencedTableName = tableName;
							column.ReferencedTableColumnName = id == null ? "Id" : id.Name;
						}
						else
						{
							column.ReferencedTableName = db.GetTableName(referencedClassType);
							var referencedColumn = db.GetColumns(referencedClassType).SingleOrDefault(x => x.IsPrimary);
							column.ReferencedTableColumnName = referencedColumn == null ? "Id" : referencedColumn.Name;
						}
						column.ForeignKeyName = GetForeignKeyName(tableName, property.Name, column.ReferencedTableName);
						column.GenerateForeignKey = referencesAttribute.GenerateForeignKey;
					}

					columns.Add(column);
				}
				Columns.TryAdd(type.TypeHandle, columns);
			}
			return columns;
		}

		#endregion

		#region Columns
		/// <summary>
		/// Generate and execute ALTER SQL needed to add a column to the database
		/// </summary>
		/// <typeparam name="T">Type representing the table to add column to</typeparam>
		/// <param name="db">Current db connection</param>
		/// <param name="expression">Property to add column for</param>
		/// <param name="defaultValue">Optional default value for column.</param>
		public static void AddColumn<T>(this IDbConnection db, Expression<Func<T, object>> expression, object defaultValue = null)
		{
			var member = GetMemberInfo(expression);
			if (member == null)
				return;

			var table = db.GetTableName<T>();
			var column = db.GetColumns<T>().SingleOrDefault(x => x.Name == member.Member.Name);
			if (column == null)
				throw new Exception("Unable to find column definition for '{0}' in table '{1}'".Fmt(member.Member.Name, table));


		    var columnExists = ColumnExists(db, column, table);
		    if(columnExists)
		    {
		        //Column already exists 
		        return;
		    }


			var sql = @"ALTER TABLE {1} ADD {0} {2}".Fmt(column.Name, table, column.SqlType);

			if (defaultValue == null)
			{
				sql += " NULL";
			}
			else
			{
				sql += " NOT NULL CONSTRAINT DF_{0}_{1} DEFAULT {2}".Fmt(table, column.Name, (column.Type == typeof(string)) ? "N'{0}'".Fmt(defaultValue) : defaultValue);
			}

			if (column.HasForeignKey && column.GenerateForeignKey)
			{
				sql += ";ALTER TABLE [{0}] ADD CONSTRAINT [{1}] FOREIGN KEY ([{2}]) REFERENCES [{3}] ([{4}]) ON UPDATE  NO ACTION ON DELETE  NO ACTION ".Fmt(
					table,
					column.ForeignKeyName,
					column.Name,
					column.ReferencedTableName,
					column.ReferencedTableColumnName
				);
			}
			sql += " END";

			db.Execute(sql);
		}

	    private static bool ColumnExists(IDbConnection db, Column column, string table)
	    {
	        string checkIfColumnExistsQuery = string.Format(@"
                SELECT 
		            IF(count(*) = 1, 'Exist','Not Exist') AS result
		        FROM
		            information_schema.columns
		        WHERE
		            -- table_schema = 'sywplatform-minimized'
		            -- AND
                        table_name = '{1}'
		                AND column_name = '{0}'", column.Name, table);
	        IEnumerable<int> results = db.Query<int>(checkIfColumnExistsQuery);

	        bool columnExists = results.FirstOrDefault() > 0;
	        return columnExists;
	    }

	    /// <summary>
		/// Generate and execute ALTER SQL needed to rename a column
		/// </summary>
		/// <typeparam name="T">Type representing the table to rename column from</typeparam>
		/// <param name="db">Current db connection</param>
		/// <param name="oldColumnName">name of column to change</param>
		/// <param name="expression">Property of new column</param>
		public static void RenameColumn<T>(this IDbConnection db, string oldColumnName, Expression<Func<T, object>> expression)
		{
			MemberExpression member = GetMemberInfo(expression);
		    if (member == null)
		    {
		        return;
		    }

		    string table = db.GetTableName<T>();
			string newColumnName = member.Member.Name;
		    var column = db.GetColumns<T>().SingleOrDefault(x => x.Name == member.Member.Name);
		    if (column != null)
		    {
		        //Let it error out if it shouldn't happen.
		        db.Execute(string.Format(@"ALTER TABLE '{0} CHANGE COLUMN {1} {2} {3};", table, oldColumnName, newColumnName,
		            column.Type));
		    }
		}
		/// <summary>
		/// Generate and execute ALTER SQL needed to drop a column from a table
		/// </summary>
		/// <typeparam name="T">Type representing the table to drop column from</typeparam>
		/// <param name="db">Current db connection</param>
		/// <param name="columnName">Name of column to be dropped</param>
		public static void DropColumn<T>(this IDbConnection db, string columnName)
		{
			var table = db.GetTableName<T>();

			var sql = @"
SET NOCOUNT ON

DECLARE @constraint sysname
DECLARE @sql nvarchar(4000)

BEGIN TRANSACTION; 

DECLARE cur_constraints CURSOR FOR
	SELECT 	Constraint_Name from [INFORMATION_SCHEMA].[CONSTRAINT_COLUMN_USAGE] where [Table_Name] = '{0}' AND [Column_Name] = '{1}'

OPEN cur_constraints

FETCH NEXT FROM cur_constraints INTO @constraint
WHILE (@@FETCH_STATUS = 0)
BEGIN
	SELECT @sql = N'ALTER TABLE [{0}] DROP CONSTRAINT '+@constraint
	EXEC sp_executesql @sql
	FETCH NEXT FROM cur_constraints INTO @constraint
END

CLOSE cur_constraints
DEALLOCATE cur_constraints

-- For some reason, some default constraints need to be handled explicitly
DECLARE cur_constraints CURSOR FOR
	SELECT 	dc.Name 
	FROM sys.tables t 
	INNER JOIN sys.default_constraints dc ON t.object_id = dc.parent_object_id 
	INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND c.column_id = dc.parent_column_id
	WHERE t.Name = '{0}' and c.Name = '{1}'

OPEN cur_constraints

FETCH NEXT FROM cur_constraints INTO @constraint
WHILE (@@FETCH_STATUS = 0)
BEGIN
	SELECT @sql = N'ALTER TABLE [{0}] DROP CONSTRAINT '+@constraint
	EXEC sp_executesql @sql
	FETCH NEXT FROM cur_constraints INTO @constraint
END

CLOSE cur_constraints
DEALLOCATE cur_constraints

IF EXISTS(select * from sys.columns where Name = N'{1}' and Object_ID = Object_ID(N'{0}'))
	ALTER TABLE [{0}] DROP COLUMN [{1}]

COMMIT TRANSACTION;".Fmt(table, columnName);

			db.Execute(sql);
		}

		#endregion

		#region Foreign Keys

		/// <summary>
		/// Get a list of foreign key names referencing the given table/column combo
		/// </summary>
		/// <param name="db">Current db connection</param>
		/// <param name="table">Table with foreign keys</param>
		/// <param name="columnName">Column with foreign keys</param>
		/// <returns></returns>
		public static List<string> GetForeignKeys(this IDbConnection db, string table, string columnName)
		{
			return db.Query<string>(String.Format(@"
				SELECT f.name AS ForeignKey
				FROM sys.foreign_keys AS f
				INNER JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id
				WHERE OBJECT_NAME(f.parent_object_id) = '{0}'
				AND COL_NAME(fc.parent_object_id, fc.parent_column_id) = '{1}'", table, columnName)).ToList();
		}

		/// <summary>
		/// Drop all foreign keys for a specific column from the given table
		/// </summary>
		/// <typeparam name="T">Type to drop foreign keys from</typeparam>
		/// <param name="db">Current db connection</param>
		/// <param name="expression">Property to drop foreign keys for</param>
		public static void DropForeignKeys<T>(this IDbConnection db, Expression<Func<T, object>> expression)
		{
			var member = GetMemberInfo(expression);
			if (member != null)
			{
				var tableName = db.GetTableName<T>();
				var columnName = member.Member.Name;

				foreach (var fk in db.GetForeignKeys(tableName, columnName))
				{
					db.DropForeignKey(tableName, fk);
				}
			}
		}

		/// <summary>
		/// Drop a foreign key from the given table
		/// </summary>
		/// <param name="db">Current db connection</param>
		/// <param name="table">Table name with associated key</param>
		/// <param name="foreignKeyName">Name of key to drop</param>
		public static void DropForeignKey(this IDbConnection db, string table, string foreignKeyName)
		{
			db.Execute(String.Format(@"
				ALTER TABLE [{1}] DROP CONSTRAINT [{0}]", foreignKeyName, table));
		}

		#endregion

		#region Scripted Objects
		/// <summary>
		/// Generate and execute SQL needed to refresh a view
		/// </summary>
		/// <param name="db">Current db connection</param>
		public static void RefreshView<T>(this IDbConnection db)
		{
			db.RefreshView(db.GetTableName<T>());
		}
		/// <summary>
		/// Generate and execute SQL needed to refresh a view
		/// </summary>
		/// <param name="db">Current db connection</param>
		/// <param name="view">Name of view to be refreshed</param>
		public static void RefreshView(this IDbConnection db, string view)
		{
		    throw new NotSupportedException("This operation: RefreshView is not supported in MySQL at this time");
//			db.Execute(String.Format(@"
//				IF EXISTS(select * from sys.views where Name = N'{0}')
//					EXEC SP_REFRESHVIEW '{0}'", view));
		}
		/// <summary>
		/// Drop and create the specified view
		/// </summary>
		/// <typeparam name="T">View to recreate</typeparam>
		/// <param name="db">Current db connection</param>
		public static void RecreateView<T>(this IDbConnection db) where T : View, new()
		{
			db.RecreateScriptedObject<T>();
		}
		/// <summary>
		/// Recreate the specified stored procedure
		/// </summary>
		/// <typeparam name="T">Stored procedure to recreate</typeparam>
		/// <param name="db">Current db connection</param>
		public static void RecreateStoredProcedure<T>(this IDbConnection db) where T : StoredProcedure, new()
		{
			db.RecreateScriptedObject<T>();
		}

		private static void RecreateScriptedObject<T>(this IDbConnection db) where T : ScriptedObject, new()
		{
			var item = new T();
			db.Execute(item.DeleteScript);
			db.Execute(item.CreateScript);
		}

		/// <summary>
		/// Recreate the <see cref="ScriptedObject"/> specified by the type parameter.
		/// </summary>
		/// <param name="type">Type of <see cref="ScriptedObject"/> (view, sproc, user defined function) to recreate</param>
		/// <param name="db">Current db connection</param>
		public static void RecreateScriptedObject(this IDbConnection db, Type type)
		{
			var item = Activator.CreateInstance(type) as ScriptedObject;
			if (item == null)
				throw new Exception("Unable to convert type to ScriptedObject: " + type);
			db.Execute(item.DeleteScript);
			db.Execute(item.CreateScript);
		}

		/// <summary>
		/// Drops all scripted objects (sprocs, views, functions)
		/// </summary>
		/// <param name="db"></param>
		public static void DropAllScriptedObjects(this IDbConnection db)
		{
			db.Execute(
@"
-- PRINT 'dropping all procedures...'
-- https://stackoverflow.com/questions/3027832/drop-all-stored-procedures-in-mysql-or-using-temporary-stored-procedures
delete from mysql.proc WHERE db LIKE 'sywplatform-minimized';

-- PRINT 'all procedures dropped.'
-- PRINT 'dropping all views...'

-- VIEWS
SET @views = NULL;
SELECT GROUP_CONCAT(table_schema, '.', table_name) INTO @views
 FROM information_schema.views
 WHERE table_schema = 'sywplatform-minimized'; -- @database_name; -- Your DB name here 

SET @views = IFNULL(CONCAT('DROP VIEW ', @views), 'SELECT ""No Views""');

PREPARE stmt FROM @views;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- PRINT 'all views dropped.'

-- PRINT 'All done'
            ");
		}
		#endregion

		private static string GetForeignKeyName(string tableName, string columnName, string referencedTableName)
		{
			return "FK_{0}_{1}__{2}".Fmt(tableName, columnName, referencedTableName);
		}

		private static string GetSqlType(Type type, PropertyInfo property)
		{
			// First check to see if SqlType attribute is applied. If so, go with that
			var sqlTypeAttribute = property.FirstAttribute<SqlTypeAttribute>(true);
			if (sqlTypeAttribute != null)
			{
				return sqlTypeAttribute.Type;
			}

			// Override any field type to map to an nvarchar field if StringLength attribute is applied to the property
			var stringLengthAttribute = property.FirstAttribute<StringLengthAttribute>(true);
			if (stringLengthAttribute != null)
			{
				return SpruceSettings.SqlSchemaTypeMap[typeof(string)].Fmt(stringLengthAttribute.MaximumLength);
			}

			string sqlType;
			var isTypeDefined = SpruceSettings.SqlSchemaTypeMap.TryGetValue(type, out sqlType);
			// Default to nvarchar(...) if the field type is not defined.
			if (!isTypeDefined)
				sqlType = SpruceSettings.SqlSchemaTypeMap[typeof(string)].Fmt(SpruceSettings.UndefinedFieldTypeStringLength.HasValue ? SpruceSettings.UndefinedFieldTypeStringLength.Value.ToString() : "MAX");

			// If we made it this far and the type is string, that means no length was specified, so default to nvarchar(max)
			if (type == typeof(string))
			{
				sqlType = sqlType.Fmt("MAX");
			}

			return sqlType;
		}

		private static MemberExpression GetMemberInfo(Expression method)
		{
			var lambda = method as LambdaExpression;
			if (lambda == null)
				throw new ArgumentNullException("method");

			MemberExpression memberExpr = null;

			switch (lambda.Body.NodeType)
			{
				case ExpressionType.Convert:
					memberExpr = ((UnaryExpression) lambda.Body).Operand as MemberExpression;
					break;
				case ExpressionType.MemberAccess:
					memberExpr = lambda.Body as MemberExpression;
					break;
			}

			if (memberExpr == null)
				throw new ArgumentException("method");

			return memberExpr;
		}

		private static readonly ConcurrentDictionary<RuntimeTypeHandle, bool> TypeExplicitColumns = new ConcurrentDictionary<RuntimeTypeHandle, bool>();
		/// <summary>
		/// Determines if the object should use explicit column names for queries
		/// </summary>
		/// <typeparam name="T">Class type</typeparam>
		/// <param name="db"></param>
		/// <returns></returns>
		internal static bool ShouldQueryExplicitColumns<T>(this IDbConnection db)
		{
			return db.ShouldQueryExplicitColumns(typeof(T));
		}
		/// <summary>
		/// Determines if the object should use explicit column names for queries
		/// </summary>
		/// <param name="type">Class type</param>
		/// <param name="db"></param>
		internal static bool ShouldQueryExplicitColumns(this IDbConnection db, Type type)
		{
			return ShouldQueryExplicitColumns(type);
		}
		/// <summary>
		/// Determines if the object should use explicit column names for queries
		/// </summary>
		/// <param name="type">Class type</param>
		private static bool ShouldQueryExplicitColumns(Type type)
		{
			var result = false;;
			if (!TypeExplicitColumns.TryGetValue(type.TypeHandle, out result))
			{
				var attribute = type.GetCustomAttributes(false).SingleOrDefault(attr => attr.GetType().Name == "QueryExplicitColumnsAttribute") as dynamic;
				result = TypeExplicitColumns[type.TypeHandle] = attribute != null;
			}
			return result;
		}
	}
}