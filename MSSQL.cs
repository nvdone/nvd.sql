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

using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace NVD.SQL
{
	/// <summary>
	/// Helper class to simplify MSSQL database operations.
	/// </summary>
	public class MSSQL : SQL
	{
		/// <summary>
		/// Creates MSSQL database connection.
		/// </summary>
		protected override void CreateConnection()
		{
			connection = new SqlConnection(ConnectionString);
			connection.Open();
		}

		/// <summary>
		/// Creates SqlCommand.
		/// </summary>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">Query parameters.</param>
		/// <returns>SqlCommand as DbCommand</returns>
		protected override DbCommand CreateCommand(string query, params object[] parameters)
		{
			SqlCommand dbCommand = new SqlCommand(query, connection as SqlConnection)
			{
				CommandType = CommandType.Text
			};

			if (transaction != null)
				dbCommand.Transaction = transaction as SqlTransaction;

			if (CommandTimeout >= 0)
				dbCommand.CommandTimeout = CommandTimeout;

			if (parameters.Length > 0)
				AppendParametersToCommand(dbCommand, parameters);

			return dbCommand;
		}

		/// <summary>
		/// Creates the data adapter of a required type
		/// </summary>
		/// <returns>SqlDataAdapter</returns>
		protected override DbDataAdapter CreateDataAdapter()
		{
			return new SqlDataAdapter();
		}

		/// <summary>
		/// Creates SQL query parameter.
		/// </summary>
		/// <returns>SqlParameter as DbParameter</returns>
		protected override DbParameter CreateParameter()
		{
			return new SqlParameter();
		}

		/// <summary>
		/// Sets the SQL parameter type and value.
		/// </summary>
		/// <param name="parameter">DB parameter.</param>
		/// <param name="typeValue">Parameter type and value.</param>
		/// <exception cref="NotImplementedException">Parameter " + parameterValue.GetType().ToString() + " type not handled!</exception>
		protected override void SetParameterTypeAndValue(DbParameter parameter, (Type ParamType, object ParamValue) typeValue)
		{
			SqlParameter sqlParameter = parameter as SqlParameter;

			if (typeValue.ParamType == typeof(int))
			{
				sqlParameter.SqlDbType = SqlDbType.Int;
			}
			else if (typeValue.ParamType == typeof(long))
			{
				sqlParameter.SqlDbType = SqlDbType.BigInt;
			}
			else if (typeValue.ParamType == typeof(string))
			{
				sqlParameter.SqlDbType = SqlDbType.NVarChar;
			}
			else if (typeValue.ParamType == typeof(DateTime))
			{
				sqlParameter.SqlDbType = SqlDbType.DateTime;
				if (((DateTime)typeValue.ParamValue).Equals(DateTime.MinValue))
					sqlParameter.Value = DBNull.Value;
				else
					sqlParameter.Value = typeValue.ParamValue;
				return;
			}
			else if (typeValue.ParamType == typeof(double))
			{
				sqlParameter.SqlDbType = SqlDbType.Float;
			}
			else if (typeValue.ParamType == typeof(decimal))
			{
				sqlParameter.SqlDbType = SqlDbType.Decimal;
			}
			else if (typeValue.ParamType == typeof(float))
			{
				sqlParameter.SqlDbType = SqlDbType.Float;
			}
			else if (typeValue.ParamType == typeof(Guid))
			{
				sqlParameter.SqlDbType = SqlDbType.UniqueIdentifier;
			}
			else if (typeValue.ParamType == typeof(bool))
			{
				sqlParameter.SqlDbType = SqlDbType.Int;
				sqlParameter.Value = ((bool)typeValue.ParamValue) ? 1 : 2;
				return;
			}
			else if (typeValue.ParamType == typeof(byte[]))
			{
				sqlParameter.SqlDbType = SqlDbType.VarBinary;
			}
			else
			{
				throw new NotImplementedException("Parameter " + typeValue.ParamType.ToString() + " type not handled!");
			}

			if(typeValue.ParamValue == null)
				sqlParameter.Value = DBNull.Value;
			else
				sqlParameter.Value = typeValue.ParamValue;
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MSSQL"/> class.
		/// </summary>
		/// <remarks>
		/// Do not forget to set ConnectionString property after construction.
		/// </remarks>
		private MSSQL() : base()
		{
			getIdentityQuery = "SELECT @@IDENTITY;";
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="MSSQL"/> class.
		/// </summary>
		/// <param name="connectionString">Connection string.</param>
		/// 
		public MSSQL(string connectionString) : this()
		{
			this.ConnectionString = connectionString;
		}
	}
}
