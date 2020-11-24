using Newtonsoft.Json.Linq;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;

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

        public DataTable QueryAsDataTable(string query)
        {
            if (string.IsNullOrWhiteSpace(query))
            {
                throw new ArgumentException($"The {nameof(query)} is null or whitespace.");
            }

            //SqlTransaction transaction = null;

            try
            {
                using (var connection = CreateDbConnection(connectionString))
                {
                    connection.Open();
                    //transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

                    using (var command = CreateDbCommand(query, connection))
                    {
                        //command.Transaction = transaction;

                        using (var reader = command.ExecuteReader())
                        {
                            var data = new DataTable(Guid.NewGuid().ToString());
                            data.Load(reader);
                            //transaction.Commit();
                            return data;
                        }
                    }
                }
            }
            catch (Exception exc)
            {
                //if (transaction != null)
                //{
                //    transaction.Rollback();
                //}

                throw;
            }
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
                var row = fillTable.NewRow();
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

        private DbCommand CreateDbCommand(string query, DbConnection connection)
        {
            switch (engineType)
            {
                case EngineType.MSSQL:
                    return new SqlCommand(query, (SqlConnection)connection);

                case EngineType.PostgreSQL:
                    return new NpgsqlCommand(query, (NpgsqlConnection)connection);

                default:
                    throw new NotImplementedException("Engine type not supported!");
            }
        }

        private void BulkCopy(string tableName, DataTable dataTable)
        {
            switch (engineType)
            {
                case EngineType.MSSQL:
                    try
                    {
                        using (var connection = new SqlConnection(connectionString))
                        {
                            connection.Open();

                            using (var sbc = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.Default))
                            {
                                //sbc.BatchSize = 1;
                                sbc.DestinationTableName = tableName;
                                sbc.WriteToServer(dataTable);
                            }
                        }
                    }
                    catch (Exception exc)
                    {
                        throw;
                    }

                    return;

                case EngineType.PostgreSQL:
                    try
                    {
                        var lsColNames = new List<string>();

                        for (int i = 0; i < dataTable.Columns.Count; i++)
                        {
                            lsColNames.Add($"\"{dataTable.Columns[i].ColumnName}\"");
                        }

                        string copyString = $"COPY \"{tableName}\" ( {string.Join(",", lsColNames) } ) FROM STDIN (FORMAT BINARY)";

                        using (var conn = new NpgsqlConnection(connectionString))
                        {
                            if (conn.State == ConnectionState.Closed)
                            {
                                conn.Open();
                            }

                            var writer = conn.BeginBinaryImport(copyString);

                            //// 1-st approach
                            //for (int i = 0; i < dataTable.Rows.Count; i++)
                            //{
                            //    writer.StartRow();
                            //    //var jRowData = JObject.FromObject(dataTable.Rows[i]);
                            //    //foreach (var kvp in jRowData)
                            //    //{
                            //    //    NpgsqlParameter colParam = GetParameter(tableName, kvp);
                            //    //    writer.Write(colParam.Value, colParam.NpgsqlDbType);
                            //    //}

                            //    var row = dataTable.Rows[i];

                            //    for (int j = 0; j < row.ItemArray.Count(); j++)
                            //    {
                            //        NpgsqlParameter colParam = GetParameter(dataTable.Columns[j], row.ItemArray[j]);
                            //        writer.Write(colParam.Value, colParam.NpgsqlDbType);
                            //    }
                            //}

                            // 2-nd approach
                            foreach (DataRow row in dataTable.Rows)
                            {
                                writer.WriteRow(row.ItemArray);
                            }

                            writer.Complete();

                            // There is no need to close the connection manually. It will be automatically closed
                            // inside Dispose method.
                        }
                    }
                    catch (Exception exc)
                    {
                        throw;
                    }

                    return;

                default:
                    throw new NotImplementedException("Engine type not supported!");
            }
        }

        //public NpgsqlParameter GetParameter(string tableName, KeyValuePair<string, JToken> columnValuePair)
        //{
        //    string columnDBypeName = ""; /*_DBTableDefProvider.GetTableColumn(tableName, columnValuePair.Key).data_type.ToLower();*/

        //    NpgsqlParameter p = new NpgsqlParameter("@" + columnValuePair.Key,
        //    columnDBypeName == "timestamp" || columnDBypeName == "timestamp without time zone" ? NpgsqlDbType.Timestamp
        //    : columnDBypeName == "timestamp with time zone" ? NpgsqlDbType.TimestampTz
        //    : columnDBypeName == "date" ? NpgsqlDbType.Date
        //    : columnDBypeName == "time" || columnDBypeName == "time without time zone" ? NpgsqlDbType.Time
        //    : columnDBypeName == "time with time zone" ? NpgsqlDbType.TimeTz
        //    : columnDBypeName == "smallint" ? NpgsqlDbType.Smallint
        //    : columnDBypeName == "integer" || columnDBypeName == "serial" ? NpgsqlDbType.Integer
        //    : columnDBypeName == "bigint" || columnDBypeName == "bigserial" ? NpgsqlDbType.Bigint
        //    : columnDBypeName == "double precision" ? NpgsqlDbType.Double
        //    : columnDBypeName == "real" ? NpgsqlDbType.Real
        //    : columnDBypeName == "boolean" ? NpgsqlDbType.Boolean
        //    : columnDBypeName == "uuid" ? NpgsqlDbType.Uuid
        //    : columnDBypeName == "bit" ? NpgsqlDbType.Bit //eg:0|1
        //    : columnDBypeName == "json" ? NpgsqlDbType.Json
        //    : columnDBypeName == "money" ? NpgsqlDbType.Money
        //    : columnDBypeName == "numeric" ? NpgsqlDbType.Numeric
        //    : columnDBypeName == "bit varying" ? NpgsqlDbType.Varbit //eg:01010101
        //    : columnDBypeName == "text" ? NpgsqlDbType.Text
        //    : columnDBypeName == "character varying" ? NpgsqlDbType.Varchar //NpgsqlDbType.Varchar可以直接用NpgsqlDbType.Text
        //    : columnDBypeName == "\"char\"" || columnDBypeName == "character" ? NpgsqlDbType.Char //NpgsqlDbType.Char可以直接用NpgsqlDbType.Text
        //                                                                                          //: columnDBypeName == "array" ? NpgsqlDbType.Array|NpgsqlDbType.Json //ARRAY需要匹配各个基础类型的Array，且不能直接以string传值，不常用不做处理
        //    : columnDBypeName == "interval" ? NpgsqlDbType.Interval
        //    //: NpgsqlDbType.Text);
        //    : NpgsqlDbType.Unknown);


        //    p.Value = columnValuePair.Value.Type == JTokenType.Null ? DBNull.Value
        //    : columnDBypeName.StartsWith("timestamp") || columnDBypeName == "date" || columnDBypeName.StartsWith("time") ? Convert.ToDateTime(((JValue)columnValuePair.Value).Value)
        //    : columnDBypeName == "smallint" ? Convert.ToInt16(columnValuePair.Value)
        //    : columnDBypeName == "integer" || columnDBypeName == "serial" ? Convert.ToInt32(((JValue)columnValuePair.Value).Value)
        //    : columnDBypeName == "bigint" || columnDBypeName == "bigserial" ? Convert.ToInt64(((JValue)columnValuePair.Value).Value)
        //    : columnDBypeName == "double precision" ? Convert.ToDouble(((JValue)columnValuePair.Value).Value)
        //    : columnDBypeName == "real" ? Convert.ToSingle(((JValue)columnValuePair.Value).Value)
        //    : columnDBypeName == "boolean" ? Convert.ToBoolean(((JValue)columnValuePair.Value).Value)
        //    : columnDBypeName == "uuid" ? Guid.Parse((string)columnValuePair.Value)
        //    : columnDBypeName == "bit" ? Convert.ToString(Convert.ToInt32(columnValuePair.Value), 2).Last().ToString()
        //    : columnDBypeName == "json" ? JObject.Parse((string)columnValuePair.Value).ToString()
        //    : columnDBypeName == "money" || columnDBypeName == "numeric" ? Convert.ToDecimal(((JValue)columnValuePair.Value).Value)
        //    : columnDBypeName == "text" || columnDBypeName == "character varying" || columnDBypeName == "character" ? (string)columnValuePair.Value
        //    : columnDBypeName == "interval" ? new TimeSpan()
        //    : (object)(string)((JValue)columnValuePair.Value).Value;
        //    //: columnDBypeName == "interval" ? TimeSpan.Parse(Regex.Replace((string)columnValuePair.Value, "days?", ".", RegexOptions.IgnoreCase).Replace(" ", ""))
        //    //: (object)(string)((JValue)columnValuePair.Value).Value;

        //    return p;
        //}

        //public NpgsqlParameter GetParameter(DataColumn dataColumn, object value)
        //{
        //    var type = dataColumn.DataType;

        //    NpgsqlParameter p = new NpgsqlParameter("@" + dataColumn.ColumnName,
        //    type == typeof(Guid) ? NpgsqlDbType.Uuid
        //    : type == typeof(string) ? NpgsqlDbType.Text
        //    : NpgsqlDbType.Unknown);

        //    p.Value = value;

        //    return p;
        //}
    }
}