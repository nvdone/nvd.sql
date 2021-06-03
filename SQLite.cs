//NVD.SQL
//Copyright © 2004-2021, Nikolay Dudkin

//This program is free software: you can redistribute it and/or modify
//it under the terms of the GNU General Public License as published by
//the Free Software Foundation, either version 3 of the License, or
//(at your option) any later version.
//This program is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
//GNU General Public License for more details.
//You should have received a copy of the GNU General Public License
//along with this program.If not, see<https://www.gnu.org/licenses/>.

using System;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;

namespace NVD.SQL
{
	/// <summary>
	/// Helper class to simplify SQLite database operations.
	/// </summary>
	public class SQLite : SQL
	{
		/// <summary>
		/// Creates SQLite database connection.
		/// </summary>
		protected override void CreateConnection()
		{
			connection = new SQLiteConnection(ConnectionString);
			connection.Open();
		}

		/// <summary>
		/// Creates SqlCommand.
		/// </summary>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">Query parameters.</param>
		/// <returns>SQLiteCommand as DbCommand</returns>
		protected override DbCommand CreateCommand(string query, params object[] parameters)
		{
			SQLiteCommand dbCommand = new SQLiteCommand(query, connection as SQLiteConnection)
			{
				CommandType = CommandType.Text
			};

			if (transaction != null)
				dbCommand.Transaction = transaction as SQLiteTransaction;

			if (CommandTimeout >= 0)
				dbCommand.CommandTimeout = CommandTimeout;

			if (parameters.Length > 0)
				AppendParametersToCommand(dbCommand, parameters);

			return dbCommand;
		}

		/// <summary>
		/// Creates the data adapter of a required type.
		/// </summary>
		/// <returns>SQLiteDataAdapter as DbDataAdapter</returns>
		protected override DbDataAdapter CreateDataAdapter()
		{
			return new SQLiteDataAdapter();
		}

		/// <summary>
		/// Creates SQL query parameter.
		/// </summary>
		/// <returns>SQLiteParameter as DbParameter</returns>
		protected override DbParameter CreateParameter()
		{
			return new SQLiteParameter();
		}

		/// <summary>
		/// Sets the SQL parameter type and value.
		/// </summary>
		/// <param name="parameter">Parameter.</param>
		/// <param name="typeValue">Parameter value.</param>
		/// <exception cref="NotImplementedException">Parameter " + parameterValue.GetType().ToString() + " type not handled!</exception>
		protected override void SetParameterTypeAndValue(DbParameter parameter, (Type ParamType, object ParamValue) typeValue)
		{
			SQLiteParameter sqlParameter = parameter as SQLiteParameter;

			if (typeValue.ParamType == typeof(int))
			{
				sqlParameter.DbType = DbType.Int32;
			}
			else if (typeValue.ParamType == typeof(long))
			{
				sqlParameter.DbType = DbType.Int64;
			}
			else if (typeValue.ParamType == typeof(string))
			{
				sqlParameter.DbType = DbType.String;
			}
			else if (typeValue.ParamType == typeof(DateTime))
			{
				sqlParameter.DbType = DbType.DateTime;
				if (((DateTime)typeValue.ParamValue).Equals(DateTime.MinValue))
					sqlParameter.Value = DBNull.Value;
				else
					sqlParameter.Value = typeValue.ParamValue;
				return;
			}
			else if (typeValue.ParamType == typeof(double))
			{
				sqlParameter.DbType = DbType.Double;
			}
			else if (typeValue.ParamType == typeof(decimal))
			{
				sqlParameter.DbType = DbType.Decimal;
			}
			else if (typeValue.ParamType == typeof(float))
			{
				sqlParameter.DbType = DbType.Double;
			}
			else if (typeValue.ParamType == typeof(Guid))
			{
				sqlParameter.DbType = DbType.Guid;
			}
			else if (typeValue.ParamType == typeof(bool))
			{
				sqlParameter.DbType = DbType.Int32;
				sqlParameter.Value = ((bool)typeValue.ParamValue) ? 1 : 2;
				return;
			}
			else if (typeValue.ParamType == typeof(byte[]))
			{
				int length = (typeValue.ParamValue as byte[]).Length;
				if (length < 8000)
				{
					sqlParameter.DbType = DbType.Binary;
					sqlParameter.Size = length;
				}
			}
			else
			{
				throw new NotImplementedException("Parameter " + typeValue.ParamType.ToString() + " type not handled!");
			}

			if (typeValue.ParamValue == null)
				sqlParameter.Value = DBNull.Value;
			else
				sqlParameter.Value = typeValue.ParamValue;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SQLite"/> class.
		/// </summary>
		/// <remarks>
		/// Do not forget to set ConnectionString property after construction.
		/// </remarks>
		private SQLite() : base()
		{
			getIdentityQuery = "SELECT last_insert_rowid();";
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SQLite"/> class.
		/// </summary>
		/// <param name="connectionString">Connection string.</param>
		/// 
		public SQLite(string connectionString) : this()
		{
			this.ConnectionString = connectionString;
		}

		/// <summary>Creates the database file</summary>
		/// <param name="path">Path to a database file.</param>
		public static void CreateDatabase(string path)
		{
			SQLiteConnection.CreateFile(path);
		}
	}
}
