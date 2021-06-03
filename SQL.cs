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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Text;

namespace NVD.SQL
{
	/// <summary>
	/// Abstract class for SQL database handling; subclass and override abstract methods.
	/// </summary>
	public abstract class SQL
	{
		#region Technical fields and properties

		/// <summary>
		/// Database connection.
		/// </summary>
		protected DbConnection connection = null;

		/// <summary>
		/// Count of active connection references. When counter reaches zero, connection is to be closed.
		/// </summary>
		protected int connectionRefs = 0;

		/// <summary>
		/// Transaction object
		/// </summary>
		protected DbTransaction transaction = null;

		/// <summary>
		/// Count of transaction references. When counter reaches zero, transaction is to be committed.
		/// Nested transactions are not supported, they are flattened into a single transaction.
		/// </summary>
		protected int transactionRefs = 0;

		/// <summary>
		/// Gets or sets the parameters prefix.
		/// </summary>
		/// <value>
		/// The parameters prefix. Default "@"
		/// </value>
		public string ParametersPrefix { get; set; }

		/// <summary>
		/// Gets or sets the colIndex of the parameters starting.
		/// </summary>
		/// <value>
		/// The starting colIndex of parameters. Default 0, i.e. @0, @1 should be used;
		/// </value>
		public int ParametersStartingIndex { get; set; }

		/// <summary>
		/// Gets or sets the command timeout, in seconds.
		/// </summary>
		/// <remarks>
		/// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
		/// The default is 30 seconds.
		/// Set this to 600 if you expect your queries to execute for a long time.
		/// </remarks>
		/// <value>The command timeout.</value>
		public int CommandTimeout { get; set; }

		/// <summary>
		/// Gets or sets the connection string.
		/// </summary>
		/// <value>The connection string.</value>
		public string ConnectionString { get; set; }

		/// <summary>Identity query.</summary>
		protected string getIdentityQuery = "";

		#endregion

		/// <summary>Initializes a new instance of the <see cref="SQL" /> class.</summary>
		protected SQL()
		{
			ParametersPrefix = "@";
		}

		#region Connection

		/// <summary>
		/// Override this function to create connection of a required type.
		/// </summary>
		protected abstract void CreateConnection();

		/// <summary>
		/// Opens connection to the database.
		/// </summary>
		/// <remarks>
		/// Opening connection before series of database operations reduces overhead.
		/// Do not forget to call <see cref="CloseConnection"/> when you are done!
		/// </remarks>
		public void OpenConnection()
		{
			if (connection == null)
			{
				CreateConnection();
			}

			connectionRefs++;
		}

		/// <summary>
		/// Closes open connection (<see cref="OpenConnection"/>) to the database.
		/// </summary>
		public void CloseConnection()
		{
			if (connection != null && --connectionRefs == 0)
			{
				connection.Close();
				connection = null;
			}
		}

		#endregion

		#region Transaction

		/// <summary>
		/// Begins the transaction.
		/// </summary>
		public void BeginTransaction()
		{
			if (connection != null)
			{
				if (transaction == null)
				{
					transaction = connection.BeginTransaction();
				}

				transactionRefs++;
			}
		}

		/// <summary>
		/// Commits transaction.
		/// </summary>
		public void CommitTransaction()
		{
			if (connection != null)
			{
				if (transaction != null && --transactionRefs == 0)
				{
					transaction.Commit();
					transaction = null;
				}
			}
		}

		/// <summary>
		/// Rollbacks transaction.
		/// </summary>
		public void RollbackTransaction()
		{
			if (connection != null && transaction != null)
			{
				transaction.Rollback();
				transaction = null;
				transactionRefs = 0;
			}
		}

		#endregion

		#region Misc abstract

		/// <summary>
		/// Override this function to create database command of a required type
		/// </summary>
		/// <param name="query">SQL query string.</param>
		/// <param name="parameters">Query parameters.</param>
		/// <returns>Descendant of DbCommand</returns>
		protected abstract DbCommand CreateCommand(string query, params object[] parameters);

		/// <summary>
		/// Override this function to create SQL command parameter.
		/// </summary>
		/// <returns>Descendant of DbParameter</returns>
		protected abstract DbParameter CreateParameter();

		/// <summary>
		/// Override this function to set adequate type (and value) of SQL command parameter.
		/// </summary>
		/// <param name="parameter">SQL command parameter.</param>
		/// <param name="typeValue">Type and value to set.</param>
		protected abstract void SetParameterTypeAndValue(DbParameter parameter, (Type ParamType, object ParamValue) typeValue);

		/// <summary>
		/// Override this function to create data adapter of a required type.
		/// </summary>
		/// <returns>Descendant of DbDataAdapter</returns>
		protected abstract DbDataAdapter CreateDataAdapter();

		#endregion

		#region Supportive

		/// <summary>
		/// Appends SQL parameters to SQL command.
		/// </summary>
		/// <param name="dbCommand">SQL command.</param>
		/// <param name="parameters">SQL parameters.</param>
		protected void AppendParametersToCommand(DbCommand dbCommand, params object[] parameters)
		{
			int ix = ParametersStartingIndex;

			for (int i = 0; i < parameters.Length; i++)
			{
				if ((parameters[i] is ICollection collection) && !(parameters[i] is byte[]))
				{
					foreach (object obj in collection)
					{
						DbParameter dbParameter = CreateParameter();
						dbParameter.ParameterName = ParametersPrefix + (ix++).ToString();

						if (obj is ValueTuple<Type, object> typeValue)
						{
							SetParameterTypeAndValue(dbParameter, (typeValue.Item1, typeValue.Item2));
						}
						else
						{
							SetParameterTypeAndValue(dbParameter, (obj.GetType(), obj));
						}

						dbCommand.Parameters.Add(dbParameter);
					}
				}
				else
				{
					DbParameter dbParameter = CreateParameter();
					dbParameter.ParameterName = ParametersPrefix + (ix++).ToString();

					if (parameters[i] is ValueTuple<Type, object> typeValue)
					{
						SetParameterTypeAndValue(dbParameter, (typeValue.Item1, typeValue.Item2));
					}
					else
					{
						SetParameterTypeAndValue(dbParameter, (parameters[i].GetType(), parameters[i]));
					}

					dbCommand.Parameters.Add(dbParameter);
				}
			}
		}

		private object hideNull(Type type, object value)
		{
			if ((value != null) && (!(value is DBNull)))
				return value;

			if (type.IsArray)
				return null;

			if (type == typeof(byte))
				return (byte)0;

			if (type == typeof(sbyte))
				return (sbyte)0;

			if (type == typeof(short))
				return (short)0;

			if (type == typeof(int))
				return (int)0;

			if (type == typeof(long))
				return (long)0;

			else if (type == typeof(string))
				return "";

			else if (type == typeof(double))
				return (double)0.0;

			else if (type == typeof(decimal))
				return (decimal)0.0m;

			else if (type == typeof(float))
				return (float)0.0;

			else if (type == typeof(DateTime))
				return DateTime.MinValue;

			else if (type == typeof(Guid))
				return Guid.Empty;

			else
				throw new NotImplementedException("Null cannot be hidden for " + type.ToString());
		}

		/// <summary>Makes parameter placeholders for IN statement.</summary>
		/// <param name="start">The starting index of a parameter.</param>
		/// <param name="count">Number of parameters within IN block.</param>
		/// <param name="separator">Item separator, comma by default.</param>
		/// <returns>
		///   A string with a list of parameter placeholders, without parenthesis.
		/// </returns>
		public string MakeInPlaceholders(int start, int count, string separator = ", ")
		{
			StringBuilder sb = new StringBuilder();

			for (int i = start; i < start + count; i++)
			{
				if (i > start)
					sb.Append(separator);
				sb.Append(this.ParametersPrefix + i.ToString());
			}

			return sb.ToString();
		}

		#endregion

		#region Basic

		/// <summary>
		/// Executes SQL query with no specific return value expected.
		/// </summary>
		/// <param name="query">The query.</param>
		/// <param name="parameters">Query parameters.</param>
		/// <returns>The number of rows affected.</returns>
		/// <remarks>
		/// Useful for DELETE and UPDATE.
		/// </remarks>
		public int Execute(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			int retval = dbCommand.ExecuteNonQuery();

			CloseConnection();

			return retval;
		}

		/// <summary>
		/// Execute SQL query and returns last row id.
		/// </summary>
		/// <param name="query">The query.</param>
		/// <param name="parameters">Query parameters.</param>
		/// <returns>id of database object.</returns>
		/// <remarks>
		/// Useful for INSERT queries, since returns id of added row.
		/// </remarks>
		public long ExecuteGetIdentity(string query, params object[] parameters)
		{
			long identity = 0;

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);
			dbCommand.ExecuteNonQuery();

			dbCommand = CreateCommand(getIdentityQuery);
			object obj = dbCommand.ExecuteScalar();
			if (obj != null && !(obj is DBNull))
			{
				try
				{
					identity = Convert.ToInt64(obj);
				}
				catch { }
			}

			CloseConnection();

			return identity;
		}

		/// <summary>
		/// Reads single value from database.
		/// </summary>
		/// <param name="query">The query.</param>
		/// <returns>Value read as generic object.</returns>
		/// <param name="parameters">Query parameters.</param>
		public object GetObject(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			object retval = dbCommand.ExecuteScalar();

			CloseConnection();

			return retval;
		}

		/// <summary>
		/// Reads single value of type T from the database.
		/// </summary>
		/// <typeparam name="T">Type of the value to read.</typeparam>
		/// <param name="query">The query.</param>
		/// <param name="parameters">The parameters.</param>
		/// <returns>T read from the database</returns>
		public T Get<T>(string query, params object[] parameters)
		{
			return (T)hideNull(typeof(T), GetObject(query, parameters));
		}

		/// <summary>
		/// Reads single value of type T from the database.
		/// </summary>
		/// <typeparam name="T">Type of the value to read.</typeparam>
		/// <param name="query">The query.</param>
		/// <param name="val">The value read.</param>
		/// <param name="parameters">The parameters.</param>
		/// <returns>false if null has been read, true otherwise</returns>
		public bool Get<T>(string query, out T val, params object[] parameters)
		{
			bool ret = true;

			object obj = GetObject(query, parameters);

			if (obj == null || obj is DBNull)
				ret = false;

			val = (T)hideNull(typeof(T), obj);

			return ret;
		}

		/// <summary>
		/// Reads byte array from database.
		/// </summary>
		/// <param name="query">The query.</param>
		/// <returns>Byte array read.</returns>
		/// <param name="parameters">Query parameters.</param>
		public byte[] GetByteArray(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			byte[] retval = (byte[])dbCommand.ExecuteScalar();

			CloseConnection();

			return retval;
		}

		/// <summary>
		/// Gets the stream to read varbinary(max) data sequentally.
		/// </summary>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">Command parameters.</param>
		/// <returns>Stream to read from.</returns>
		/// <remarks>
		/// SELECT single column in a query. We are making stream for column 0 here.
		/// </remarks>
		public Stream GetBinaryStream(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			if (dr.Read() == true)
			{
				return new BinaryDataStream(this, dr, 0);
			}
			else
			{
				dr.Close();
				CloseConnection();
				return null;
			}
		}

		/// <summary>
		/// Reads number of row fields as array of objects.
		/// </summary>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">Command parameters.</param>
		/// <returns>Row fields as array of objects</returns>
		public object[] GetFields(string query, params object[] parameters)
		{
			object[] result = null;

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			if (dr.Read() == true)
			{
				result = new object[dr.FieldCount];

				for (int i = 0; i < dr.FieldCount; i++)
					result[i] = hideNull(dr.GetValue(i).GetType(), dr.GetValue(i));
			}

			dr.Close();

			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads the List of Ts from the database.
		/// </summary>
		/// <typeparam name="T">Type of List element to read.</typeparam>
		/// <param name="query">The query.</param>
		/// <param name="parameters">The parameters.</param>
		/// <returns>List of Ts read.</returns>
		public List<T> GetList<T>(string query, params object[] parameters)
		{
			List<T> list = new List<T>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				list.Add((T)hideNull(typeof(T), dr.GetValue(0)));
			}

			dr.Close();

			CloseConnection();

			return list;
		}

		/// <summary>
		/// Reads data dbTable.
		/// </summary>
		/// <param name="query">The query.</param>
		/// <param name="parameters">Query parameters.</param>
		/// <returns>Data read as DataTable.</returns>
		public DataTable GetDataTable(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataAdapter myAdapter = CreateDataAdapter();

			DataTable dt = new DataTable
			{
				Locale = System.Globalization.CultureInfo.InvariantCulture
			};

			myAdapter.SelectCommand = dbCommand;

			myAdapter.Fill(dt);

			CloseConnection();

			return dt;
		}

		#endregion

		#region Dictionaries, Tuples, Lists

		/// <summary>
		/// Reads a number of fields as a Tuple.
		/// </summary>
		/// <typeparam name="T1">The type of the field 1.</typeparam>
		/// <typeparam name="T2">The type of the field 2.</typeparam>
		/// <param name="query">Sql query.</param>
		/// <param name="parameters">Sql command parameters.</param>
		/// <returns>Fields as a tuple</returns>
		public Tuple<T1, T2> GetTuple<T1, T2>(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			if (dr.Read() == true)
			{
				var result = new Tuple<T1, T2>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)));

				dr.Close();
				CloseConnection();

				return result;
			}

			dr.Close();
			CloseConnection();

			return new Tuple<T1, T2>((T1)hideNull(typeof(T1), null), (T2)hideNull(typeof(T2), null));

		}

		/// <summary>
		/// Reads a number of fields as a Tuple.
		/// </summary>
		/// <typeparam name="T1">The type of the field 1.</typeparam>
		/// <typeparam name="T2">The type of the field 2.</typeparam>
		/// <typeparam name="T3">The type of the field 3.</typeparam>
		/// <param name="query">Sql query.</param>
		/// <param name="parameters">Sql command parameters.</param>
		/// <returns>Fields as a tuple</returns>
		public Tuple<T1, T2, T3> GetTuple<T1, T2, T3>(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			if (dr.Read() == true)
			{
				var result = new Tuple<T1, T2, T3>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)));

				dr.Close();
				CloseConnection();

				return result;
			}

			dr.Close();
			CloseConnection();

			return new Tuple<T1, T2, T3>((T1)hideNull(typeof(T1), null), (T2)hideNull(typeof(T2), null), (T3)hideNull(typeof(T3), null));

		}

		/// <summary>
		/// Reads a number of fields as a Tuple.
		/// </summary>
		/// <typeparam name="T1">The type of the field 1.</typeparam>
		/// <typeparam name="T2">The type of the field 2.</typeparam>
		/// <typeparam name="T3">The type of the field 3.</typeparam>
		/// <typeparam name="T4">The type of the field 4.</typeparam>
		/// <param name="query">Sql query.</param>
		/// <param name="parameters">Sql command parameters.</param>
		/// <returns>Fields as a tuple</returns>
		public Tuple<T1, T2, T3, T4> GetTuple<T1, T2, T3, T4>(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			if (dr.Read() == true)
			{
				var result = new Tuple<T1, T2, T3, T4>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)));

				dr.Close();
				CloseConnection();

				return result;
			}

			dr.Close();
			CloseConnection();

			return new Tuple<T1, T2, T3, T4>((T1)hideNull(typeof(T1), null), (T2)hideNull(typeof(T2), null), (T3)hideNull(typeof(T3), null), (T4)hideNull(typeof(T4), null));

		}

		/// <summary>
		/// Reads a number of fields as a Tuple.
		/// </summary>
		/// <typeparam name="T1">The type of the field 1.</typeparam>
		/// <typeparam name="T2">The type of the field 2.</typeparam>
		/// <typeparam name="T3">The type of the field 3.</typeparam>
		/// <typeparam name="T4">The type of the field 4.</typeparam>
		/// <typeparam name="T5">The type of the field 5.</typeparam>
		/// <param name="query">Sql query.</param>
		/// <param name="parameters">Sql command parameters.</param>
		/// <returns>
		/// Fields as a tuple
		/// </returns>
		public Tuple<T1, T2, T3, T4, T5> GetTuple<T1, T2, T3, T4, T5>(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			if (dr.Read() == true)
			{
				var result = new Tuple<T1, T2, T3, T4, T5>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)));

				dr.Close();
				CloseConnection();

				return result;
			}

			dr.Close();
			CloseConnection();

			return new Tuple<T1, T2, T3, T4, T5>((T1)hideNull(typeof(T1), null), (T2)hideNull(typeof(T2), null), (T3)hideNull(typeof(T3), null), (T4)hideNull(typeof(T4), null), (T5)hideNull(typeof(T5), null));
		}

		/// <summary>
		/// Reads a number of fields as a Tuple.
		/// </summary>
		/// <typeparam name="T1">The type of the field 1.</typeparam>
		/// <typeparam name="T2">The type of the field 2.</typeparam>
		/// <typeparam name="T3">The type of the field 3.</typeparam>
		/// <typeparam name="T4">The type of the field 4.</typeparam>
		/// <typeparam name="T5">The type of the field 5.</typeparam>
		/// <typeparam name="T6">The type of the field 6.</typeparam>
		/// <param name="query">Sql query.</param>
		/// <param name="parameters">Sql command parameters.</param>
		/// <returns>
		/// Fields as a tuple
		/// </returns>
		public Tuple<T1, T2, T3, T4, T5, T6> GetTuple<T1, T2, T3, T4, T5, T6>(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			if (dr.Read() == true)
			{
				var result = new Tuple<T1, T2, T3, T4, T5, T6>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)), (T6)hideNull(typeof(T6), dr.GetValue(5)));

				dr.Close();
				CloseConnection();

				return result;

			}

			dr.Close();
			CloseConnection();

			return new Tuple<T1, T2, T3, T4, T5, T6>((T1)hideNull(typeof(T1), null), (T2)hideNull(typeof(T2), null), (T3)hideNull(typeof(T3), null), (T4)hideNull(typeof(T4), null), (T5)hideNull(typeof(T5), null), (T6)hideNull(typeof(T6), null));

		}

		/// <summary>
		/// Reads a number of fields as a Tuple.
		/// </summary>
		/// <typeparam name="T1">The type of the field 1.</typeparam>
		/// <typeparam name="T2">The type of the field 2.</typeparam>
		/// <typeparam name="T3">The type of the field 3.</typeparam>
		/// <typeparam name="T4">The type of the field 4.</typeparam>
		/// <typeparam name="T5">The type of the field 5.</typeparam>
		/// <typeparam name="T6">The type of the field 6.</typeparam>
		/// <typeparam name="T7">The type of the field 7.</typeparam>
		/// <param name="query">Sql query.</param>
		/// <param name="parameters">Sql command parameters.</param>
		/// <returns>
		/// Fields as a tuple
		/// </returns>
		public Tuple<T1, T2, T3, T4, T5, T6, T7> GetTuple<T1, T2, T3, T4, T5, T6, T7>(string query, params object[] parameters)
		{
			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			if (dr.Read() == true)
			{
				var result = new Tuple<T1, T2, T3, T4, T5, T6, T7>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)), (T6)hideNull(typeof(T6), dr.GetValue(5)), (T7)hideNull(typeof(T7), dr.GetValue(6)));

				dr.Close();
				CloseConnection();

				return result;
			}

			dr.Close();
			CloseConnection();

			return new Tuple<T1, T2, T3, T4, T5, T6, T7>((T1)hideNull(typeof(T1), null), (T2)hideNull(typeof(T2), null), (T3)hideNull(typeof(T3), null), (T4)hideNull(typeof(T4), null), (T5)hideNull(typeof(T5), null), (T6)hideNull(typeof(T6), null), (T7)hideNull(typeof(T7), null));

		}

		/// <summary>
		/// Reads two columns from the database as Dictionary.
		/// </summary>
		/// <typeparam name="T1">The type of the key.</typeparam>
		/// <typeparam name="T2">The type of the value.</typeparam>
		/// <param name="query">The query.</param>
		/// <param name="parameters">The parameters.</param>
		/// <returns>Two columns as key-value pairs in the form of Dictionary.</returns>
		public Dictionary<T1, T2> GetDictionary<T1, T2>(string query, params object[] parameters)
		{
			Dictionary<T1, T2> result = new Dictionary<T1, T2>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				if (!result.ContainsKey((T1)dr.GetValue(0)))
					result.Add((T1)dr.GetValue(0), (T2)hideNull(typeof(T2), dr.GetValue(1)));
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads the Dictionary of Lists
		/// </summary>
		/// <typeparam name="T1">The type of key</typeparam>
		/// <typeparam name="T2">The type of List value</typeparam>
		/// <param name="query">SQL query</param>
		/// <param name="parameters">SQL query parameters</param>
		/// <returns>Dictionary of Lists</returns>
		public Dictionary<T1, List<T2>> GetListDictionary<T1, T2>(string query, params object[] parameters)
		{
			T1 key;
			T2 value;
			Dictionary<T1, List<T2>> result = new Dictionary<T1, List<T2>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);
				value = (T2)hideNull(typeof(T2), dr.GetValue(1));

				if (!result.ContainsKey(key))
				{
					result.Add(key, new List<T2>());
				}

				result[key].Add(value);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 4.</typeparam>
		/// <typeparam name="T6">The type of the tuple element no. 5.</typeparam>
		/// <typeparam name="T7">The type of the tuple element no. 6.</typeparam>
		/// <typeparam name="T8">The type of the tuple element no. 7.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Tuples.</returns>
		public Dictionary<T1, Tuple<T2, T3, T4, T5, T6, T7, T8>> GetTupleDictionary<T1, T2, T3, T4, T5, T6, T7, T8>(string query, params object[] parameters)
		{
			T1 key;
			Dictionary<T1, Tuple<T2, T3, T4, T5, T6, T7, T8>> result = new Dictionary<T1, Tuple<T2, T3, T4, T5, T6, T7, T8>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3, T4, T5, T6, T7, T8>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)), (T6)hideNull(typeof(T6), dr.GetValue(5)), (T7)hideNull(typeof(T7), dr.GetValue(6)), (T8)hideNull(typeof(T8), dr.GetValue(7)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, tuple);
				}

			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 4.</typeparam>
		/// <typeparam name="T6">The type of the tuple element no. 5.</typeparam>
		/// <typeparam name="T7">The type of the tuple element no. 6.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Tuples.</returns>
		public Dictionary<T1, Tuple<T2, T3, T4, T5, T6, T7>> GetTupleDictionary<T1, T2, T3, T4, T5, T6, T7>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, Tuple<T2, T3, T4, T5, T6, T7>> result = new Dictionary<T1, Tuple<T2, T3, T4, T5, T6, T7>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3, T4, T5, T6, T7>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)), (T6)hideNull(typeof(T6), dr.GetValue(5)), (T7)hideNull(typeof(T7), dr.GetValue(6)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, tuple);
				}

			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 4.</typeparam>
		/// <typeparam name="T6">The type of the tuple element no. 5.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Tuples.</returns>
		public Dictionary<T1, Tuple<T2, T3, T4, T5, T6>> GetTupleDictionary<T1, T2, T3, T4, T5, T6>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, Tuple<T2, T3, T4, T5, T6>> result = new Dictionary<T1, Tuple<T2, T3, T4, T5, T6>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3, T4, T5, T6>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)), (T6)hideNull(typeof(T6), dr.GetValue(5)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, tuple);
				}

			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 4.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Tuples.</returns>
		public Dictionary<T1, Tuple<T2, T3, T4, T5>> GetTupleDictionary<T1, T2, T3, T4, T5>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, Tuple<T2, T3, T4, T5>> result = new Dictionary<T1, Tuple<T2, T3, T4, T5>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3, T4, T5>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, tuple);
				}

			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 3.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Tuples.</returns>
		public Dictionary<T1, Tuple<T2, T3, T4>> GetTupleDictionary<T1, T2, T3, T4>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, Tuple<T2, T3, T4>> result = new Dictionary<T1, Tuple<T2, T3, T4>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3, T4>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, tuple);
				}

			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Tuples.</returns>
		public Dictionary<T1, Tuple<T2, T3>> GetTupleDictionary<T1, T2, T3>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, Tuple<T2, T3>> result = new Dictionary<T1, Tuple<T2, T3>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, tuple);
				}

			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 4.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 5.</typeparam>
		/// <typeparam name="T6">The type of the tuple element no. 6.</typeparam>
		/// <typeparam name="T7">The type of the tuple element no. 7.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>List of Tuples.</returns>
		public List<Tuple<T1, T2, T3, T4, T5, T6, T7>> GetTupleList<T1, T2, T3, T4, T5, T6, T7>(string query, params object[] parameters)
		{
			List<Tuple<T1, T2, T3, T4, T5, T6, T7>> result = new List<Tuple<T1, T2, T3, T4, T5, T6, T7>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				var tuple = new Tuple<T1, T2, T3, T4, T5, T6, T7>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)), (T6)hideNull(typeof(T6), dr.GetValue(5)), (T7)hideNull(typeof(T7), dr.GetValue(6)));

				result.Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 4.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 5.</typeparam>
		/// <typeparam name="T6">The type of the tuple element no. 6.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>List of Tuples.</returns>
		public List<Tuple<T1, T2, T3, T4, T5, T6>> GetTupleList<T1, T2, T3, T4, T5, T6>(string query, params object[] parameters)
		{
			List<Tuple<T1, T2, T3, T4, T5, T6>> result = new List<Tuple<T1, T2, T3, T4, T5, T6>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				var tuple = new Tuple<T1, T2, T3, T4, T5, T6>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)), (T6)hideNull(typeof(T6), dr.GetValue(5)));

				result.Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 4.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 5.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>List of Tuples.</returns>
		public List<Tuple<T1, T2, T3, T4, T5>> GetTupleList<T1, T2, T3, T4, T5>(string query, params object[] parameters)
		{
			List<Tuple<T1, T2, T3, T4, T5>> result = new List<Tuple<T1, T2, T3, T4, T5>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				var tuple = new Tuple<T1, T2, T3, T4, T5>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)));

				result.Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 4.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>List of Tuples.</returns>
		public List<Tuple<T1, T2, T3, T4>> GetTupleList<T1, T2, T3, T4>(string query, params object[] parameters)
		{
			List<Tuple<T1, T2, T3, T4>> result = new List<Tuple<T1, T2, T3, T4>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				var tuple = new Tuple<T1, T2, T3, T4>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)));

				result.Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 3.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>List of Tuples.</returns>
		public List<Tuple<T1, T2, T3>> GetTupleList<T1, T2, T3>(string query, params object[] parameters)
		{
			List<Tuple<T1, T2, T3>> result = new List<Tuple<T1, T2, T3>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				var tuple = new Tuple<T1, T2, T3>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)));

				result.Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 2.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>List of Tuples.</returns>
		public List<Tuple<T1, T2>> GetTupleList<T1, T2>(string query, params object[] parameters)
		{
			List<Tuple<T1, T2>> result = new List<Tuple<T1, T2>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				var tuple = new Tuple<T1, T2>((T1)hideNull(typeof(T1), dr.GetValue(0)), (T2)hideNull(typeof(T2), dr.GetValue(1)));

				result.Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 4.</typeparam>
		/// <typeparam name="T6">The type of the tuple element no. 5.</typeparam>
		/// <typeparam name="T7">The type of the tuple element no. 6.</typeparam>
		/// <typeparam name="T8">The type of the tuple element no. 7.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Lists of Tuples.</returns>
		public Dictionary<T1, List<Tuple<T2, T3, T4, T5, T6, T7, T8>>> GetTupleListDictionary<T1, T2, T3, T4, T5, T6, T7, T8>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, List<Tuple<T2, T3, T4, T5, T6, T7, T8>>> result = new Dictionary<T1, List<Tuple<T2, T3, T4, T5, T6, T7, T8>>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3, T4, T5, T6, T7, T8>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)), (T6)hideNull(typeof(T6), dr.GetValue(5)), (T7)hideNull(typeof(T7), dr.GetValue(6)), (T8)hideNull(typeof(T8), dr.GetValue(7)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, new List<Tuple<T2, T3, T4, T5, T6, T7, T8>>());
				}

				result[key].Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 4.</typeparam>
		/// <typeparam name="T6">The type of the tuple element no. 5.</typeparam>
		/// <typeparam name="T7">The type of the tuple element no. 6.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Lists of Tuples.</returns>
		public Dictionary<T1, List<Tuple<T2, T3, T4, T5, T6, T7>>> GetTupleListDictionary<T1, T2, T3, T4, T5, T6, T7>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, List<Tuple<T2, T3, T4, T5, T6, T7>>> result = new Dictionary<T1, List<Tuple<T2, T3, T4, T5, T6, T7>>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3, T4, T5, T6, T7>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)), (T6)hideNull(typeof(T6), dr.GetValue(5)), (T7)hideNull(typeof(T7), dr.GetValue(6)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, new List<Tuple<T2, T3, T4, T5, T6, T7>>());
				}

				result[key].Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 4.</typeparam>
		/// <typeparam name="T6">The type of the tuple element no. 5.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Lists of Tuples.</returns>
		public Dictionary<T1, List<Tuple<T2, T3, T4, T5, T6>>> GetTupleListDictionary<T1, T2, T3, T4, T5, T6>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, List<Tuple<T2, T3, T4, T5, T6>>> result = new Dictionary<T1, List<Tuple<T2, T3, T4, T5, T6>>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3, T4, T5, T6>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)), (T6)hideNull(typeof(T6), dr.GetValue(5)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, new List<Tuple<T2, T3, T4, T5, T6>>());
				}

				result[key].Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 3.</typeparam>
		/// <typeparam name="T5">The type of the tuple element no. 4.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Lists of Tuples.</returns>
		public Dictionary<T1, List<Tuple<T2, T3, T4, T5>>> GetTupleListDictionary<T1, T2, T3, T4, T5>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, List<Tuple<T2, T3, T4, T5>>> result = new Dictionary<T1, List<Tuple<T2, T3, T4, T5>>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3, T4, T5>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)), (T5)hideNull(typeof(T5), dr.GetValue(4)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, new List<Tuple<T2, T3, T4, T5>>());
				}

				result[key].Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <typeparam name="T4">The type of the tuple element no. 3.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Lists of Tuples.</returns>
		public Dictionary<T1, List<Tuple<T2, T3, T4>>> GetTupleListDictionary<T1, T2, T3, T4>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, List<Tuple<T2, T3, T4>>> result = new Dictionary<T1, List<Tuple<T2, T3, T4>>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3, T4>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)), (T4)hideNull(typeof(T4), dr.GetValue(3)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, new List<Tuple<T2, T3, T4>>());
				}

				result[key].Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		/// <summary>
		/// Reads Dictionary of List of Tuples
		/// </summary>
		/// <typeparam name="T1">The type of the key</typeparam>
		/// <typeparam name="T2">The type of the tuple element no. 1.</typeparam>
		/// <typeparam name="T3">The type of the tuple element no. 2.</typeparam>
		/// <param name="query">SQL query.</param>
		/// <param name="parameters">SQL query parameters.</param>
		/// <returns>Dictionary of Lists of Tuples.</returns>
		public Dictionary<T1, List<Tuple<T2, T3>>> GetTupleListDictionary<T1, T2, T3>(string query, params object[] parameters)
		{
			T1 key;

			Dictionary<T1, List<Tuple<T2, T3>>> result = new Dictionary<T1, List<Tuple<T2, T3>>>();

			OpenConnection();

			DbCommand dbCommand = CreateCommand(query, parameters);

			DbDataReader dr = dbCommand.ExecuteReader();

			while (dr.Read() == true)
			{
				if (dr.IsDBNull(0))
					continue;

				key = (T1)dr.GetValue(0);

				var tuple = new Tuple<T2, T3>((T2)hideNull(typeof(T2), dr.GetValue(1)), (T3)hideNull(typeof(T3), dr.GetValue(2)));

				if (!result.ContainsKey(key))
				{
					result.Add(key, new List<Tuple<T2, T3>>());
				}

				result[key].Add(tuple);
			}

			dr.Close();
			CloseConnection();

			return result;

		}

		#endregion
	}
}
