using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using Z.BulkOperations;

namespace FunWithSqlBulkCopy
{
    public class Helper
    {
        private readonly string connectionString;
        private readonly EngineType engineType;

        public Helper(string connectionString)
        {
            this.connectionString = connectionString;
            this.engineType = connectionString.Contains("postgres")
                ? EngineType.PostgreSQL
                : EngineType.MSSQL;
        }

        public void BulkCopy<T>(string tableName, IEnumerable<T> collection, IDictionary<string, Type> mappings)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentException($"The {nameof(tableName)} is null or whitespace.");
            }

            if (collection == null || !collection.Any())
            {
                throw new ArgumentException($"The {nameof(collection)} is null or empty.");
            }

            if (mappings == null || !mappings.Any())
            {
                throw new ArgumentException($"The {nameof(mappings)} is null or empty.");
            }

            if (engineType == EngineType.PostgreSQL)
            {
                tableName = tableName.ToLower();
            }

            var fillTable = CreateFillTable<T>(collection, mappings);

            BulkCopy(tableName, fillTable);
        }

        private DataTable CreateFillTable<T>(IEnumerable<T> collection, IDictionary<string, Type> mappings)
        {
            var fillTable = new DataTable();

            foreach (var item in mappings)
            {
                string columnName = engineType == EngineType.PostgreSQL
                    ? item.Key.ToLower()
                    : item.Key;

                fillTable.Columns.Add(columnName, item.Value);
            }

            foreach (var item in collection)
            {
                fillTable.NewRow();
                fillTable.Rows.Add(GetProprValues(item, mappings));
            }

            return fillTable;
        }

        private static object GetPropValue(object src, string propName)
        {
            return src.GetType().GetProperty(propName).GetValue(src, null);
        }

        private static object[] GetProprValues(object src, IDictionary<string, Type> mappings)
        {
            var list = new List<object>();

            foreach (var item in mappings)
            {
                list.Add(GetPropValue(src, item.Key));
            }

            return list.ToArray();
        }

        private DbConnection CreateDbConnection(string connectionString)
        {
            switch (engineType)
            {
                case EngineType.MSSQL:
                    return new SqlConnection(connectionString);

                case EngineType.PostgreSQL:
                    return new NpgsqlConnection(connectionString);

                default:
                    throw new NotImplementedException("Engine type not supported!");
            }
        }

        private void BulkCopy(string tableName, DataTable dataTable)
        {
            try
            {
                using (var connection = CreateDbConnection(connectionString))
                {
                    if (connection.State == ConnectionState.Closed)
                    {
                        connection.Open();
                    }

                    using (var bulk = new BulkOperation(connection))
                    {
                        bulk.DestinationTableName = tableName;
                        bulk.BulkInsert(dataTable);
                    }
                }
            }
            catch (Exception exc)
            {
                // TODO: Add logging.
                throw;
            }
        }
    }
}