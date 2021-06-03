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
using System.Data.Common;
using System.IO;

namespace NVD.SQL
{
	/// <summary>
	/// Allows to read varbinary(max) sequentally
	/// </summary>
	/// <seealso cref="System.IO.Stream" />
	internal class BinaryDataStream : Stream
	{
		private SQL db;
		private DbDataReader reader;
		private readonly int columnIndex;
		private long position;

		/// <summary>
		/// Initializes a new instance of the <see cref="BinaryDataStream"/> class.
		/// </summary>
		/// <param name="db">Reference to MSSQL in order to call CloseConnection.</param>
		/// <param name="reader">SqlDataReader used for reading.</param>
		/// <param name="columnIndex">Index of the column to be read.</param>
		public BinaryDataStream(SQL db, DbDataReader reader, int columnIndex)
		{
			this.db = db;
			this.reader = reader;
			this.columnIndex = columnIndex;
		}

		/// <summary>
		/// Gets the position within the current stream.
		/// </summary>
		/// <exception cref="NotImplementedException"></exception>
		public override long Position
		{
			get { return position; }
			set { throw new NotImplementedException(); }
		}

		/// <summary>
		/// Reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read.
		/// </summary>
		/// <param name="buffer">An array of bytes. When this method returns, the buffer contains the specified byte array with the values between <paramref name="offset" /> and (<paramref name="offset" /> + <paramref name="count" /> - 1) replaced by the bytes read from the current source.</param>
		/// <param name="offset">The zero-based byte offset in <paramref name="buffer" /> at which to begin storing the data read from the current stream.</param>
		/// <param name="count">The maximum number of bytes to be read from the current stream.</param>
		/// <returns>
		/// The total number of bytes read into the buffer. This can be less than the number of bytes requested if that many bytes are not currently available, or zero (0) if the end of the stream has been reached.
		/// </returns>
		public override int Read(byte[] buffer, int offset, int count)
		{
			if (db == null)
				return 0;

			int read = (int) reader.GetBytes(columnIndex, position, buffer, offset, count);
			position += read;

			if (read == 0)
			{
				db.CloseConnection();
				db = null;
			}

			return read;
		}

		/// <summary>
		/// Gets a value indicating whether the current stream supports reading.
		/// </summary>
		public override bool CanRead
		{
			get { return true; }
		}

		/// <summary>
		/// Gets a value indicating whether the current stream supports seeking.
		/// </summary>
		public override bool CanSeek
		{
			get { return false; }
		}

		/// <summary>
		/// Gets a value indicating whether the current stream supports writing.
		/// </summary>
		public override bool CanWrite
		{
			get { return false; }
		}

		/// <summary>
		/// Clears all buffers for this stream and causes any buffered data to be written to the underlying device.
		/// </summary>
		/// <exception cref="NotImplementedException"></exception>
		public override void Flush()
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the length in bytes of the stream.
		/// </summary>
		/// <exception cref="NotImplementedException"></exception>
		public override long Length
		{
			get { throw new NotImplementedException(); }
		}

		/// <summary>
		/// Ssets the position within the current stream.
		/// </summary>
		/// <param name="offset">A byte offset relative to the <paramref name="origin" /> parameter.</param>
		/// <param name="origin">A value of type <see cref="T:System.IO.SeekOrigin" /> indicating the reference point used to obtain the new position.</param>
		/// <returns>
		/// The new position within the current stream.
		/// </returns>
		/// <exception cref="NotImplementedException"></exception>
		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Sets the length of the current stream.
		/// </summary>
		/// <param name="value">The desired length of the current stream in bytes.</param>
		/// <exception cref="NotImplementedException"></exception>
		public override void SetLength(long value)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Writes a sequence of bytes to the current stream and advances the current position within this stream by the number of bytes written.
		/// </summary>
		/// <param name="buffer">An array of bytes. This method copies <paramref name="count" /> bytes from <paramref name="buffer" /> to the current stream.</param>
		/// <param name="offset">The zero-based byte offset in <paramref name="buffer" /> at which to begin copying bytes to the current stream.</param>
		/// <param name="count">The number of bytes to be written to the current stream.</param>
		/// <exception cref="NotImplementedException"></exception>
		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Releases the unmanaged resources used by the <see cref="T:System.IO.Stream" /> and optionally releases the managed resources.
		/// </summary>
		/// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
		protected override void Dispose(bool disposing)
		{
			if (disposing)
			{
				db?.CloseConnection();
				db = null;
				reader?.Dispose();
				reader = null;
			}

			base.Dispose(disposing);
		}
	}
}
