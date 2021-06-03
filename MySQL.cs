// NVD.SQL
// Copyright © 2004-2021, Nikolay Dudkin

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
// GNU General Public License for more details.
// You should have received a copy of the GNU General Public License
// along with this program.If not, see<https://www.gnu.org/licenses/>.

using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Data.Common;

namespace NVD.SQL
{
	/// <summary>
	/// Helper class to simplify MySQL database operations.
	/// </summary>
	public class MySQL : SQL
	{
		/// <summary>
		/// Creates MySQL database connection.
		/// </summary>
		protected override void CreateConnection()
		{
			connection = new MySqlConnection(ConnectionString);
			connection.Open();
		}

		/// <summary>
		/// Creates SqlCommand.
		/// </summary>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">Query parameters.</param>
		/// <returns>MySqlCommand as DbCommand</returns>
		protected override DbCommand CreateCommand(string query, params object[] parameters)
		{
			MySqlCommand dbCommand = new MySqlCommand(query, connection as MySqlConnection)
			{
				CommandType = CommandType.Text
			};

			if (transaction != null)
				dbCommand.Transaction = transaction as MySqlTransaction;

			if (CommandTimeout >= 0)
				dbCommand.CommandTimeout = CommandTimeout;

			if (parameters.Length > 0)
				AppendParametersToCommand(dbCommand, parameters);

			return dbCommand;
		}

		/// <summary>
		/// Creates the data adapter of a required type.
		/// </summary>
		/// <returns>MySqlDataAdapter as DbDataAdapter</returns>
		protected override DbDataAdapter CreateDataAdapter()
		{
			return new MySqlDataAdapter();
		}

		/// <summary>
		/// Creates SQL query parameter.
		/// </summary>
		/// <returns>MySqlParameter as DbParameter</returns>
		protected override DbParameter CreateParameter()
		{
			return new MySqlParameter();
		}

		/// <summary>
		/// Sets the SQL parameter type and value.
		/// </summary>
		/// <param name="parameter">Parameter.</param>
		/// <param name="typeValue">Parameter value.</param>
		/// <exception cref="NotImplementedException">Parameter " + parameterValue.GetType().ToString() + " type not handled!</exception>
		protected override void SetParameterTypeAndValue(DbParameter parameter, (Type ParamType, object ParamValue) typeValue)
		{
			MySqlParameter sqlParameter = parameter as MySqlParameter;

			if (typeValue.ParamType == typeof(int))
			{
				sqlParameter.MySqlDbType = MySqlDbType.Int32;
			}
			else if (typeValue.ParamType == typeof(long))
			{
				sqlParameter.MySqlDbType = MySqlDbType.Int64;
			}
			else if (typeValue.ParamType == typeof(string))
			{
				sqlParameter.MySqlDbType = MySqlDbType.VarString;
			}
			else if (typeValue.ParamType == typeof(DateTime))
			{
				sqlParameter.MySqlDbType = MySqlDbType.DateTime;
				if (((DateTime)typeValue.ParamValue).Equals(DateTime.MinValue))
					sqlParameter.Value = DBNull.Value;
				else
					sqlParameter.Value = typeValue.ParamValue;
				return;
			}
			else if (typeValue.ParamType == typeof(double))
			{
				sqlParameter.MySqlDbType = MySqlDbType.Float;
			}
			else if (typeValue.ParamType == typeof(decimal))
			{
				sqlParameter.MySqlDbType = MySqlDbType.Decimal;
			}
			else if (typeValue.ParamType == typeof(float))
			{
				sqlParameter.MySqlDbType = MySqlDbType.Float;
			}
			else if (typeValue.ParamType == typeof(Guid))
			{
				sqlParameter.MySqlDbType = MySqlDbType.Guid;
			}
			else if (typeValue.ParamType == typeof(bool))
			{
				sqlParameter.MySqlDbType = MySqlDbType.Int32;
				sqlParameter.Value = ((bool)typeValue.ParamValue) ? 1 : 2;
				return;
			}
			else if (typeValue.ParamType == typeof(byte[]))
			{
				int length = (typeValue.ParamValue as byte[]).Length;
				if (length > 16277215)
					sqlParameter.MySqlDbType = MySqlDbType.LongBlob;
				else if (length > 65535)
					sqlParameter.MySqlDbType = MySqlDbType.MediumBlob;
				else
					sqlParameter.MySqlDbType = MySqlDbType.Blob;
				sqlParameter.Size = length;
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
		/// Initializes a new instance of the <see cref="MySQL"/> class.
		/// </summary>
		/// <remarks>
		/// Do not forget to set ConnectionString property after construction.
		/// </remarks>
		private MySQL() : base()
		{
			getIdentityQuery = "SELECT @@IDENTITY;";
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MySQL"/> class.
		/// </summary>
		/// <param name="connectionString">Connection string.</param>
		/// 
		public MySQL(string connectionString) : this()
		{
			this.ConnectionString = connectionString;
		}
	}
}
