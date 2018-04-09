using System;
using System.Data;

namespace SqlServiceBrokerUtil
{
    public static class IDataReaderExtensions
    {
        public static long GetByteLength(this IDataReader reader, string columnName)
        {
            int column = reader.GetOrdinal(columnName);
            return reader.GetBytes(column, 0, null, 0, 0);
        }

        public static byte[] GetBytes(this IDataReader reader, string columnName)
        {
            byte[] buffer = new byte[0];
            int column = reader.GetOrdinal(columnName);
            if (!reader.IsDBNull(column))
            {
                long length = reader.GetByteLength(columnName);
                buffer = new byte[length];
                int position = 0;
                long bytesRead = 0;
                int bufferSize = 1024;
                while (bytesRead < length)
                {
                    bytesRead += reader.GetBytes(column, position, buffer, position, bufferSize);
                    position += bufferSize;
                }                
            }
            return buffer;
        }

        public static DateTime? GetDateTime(this IDataReader reader, string columnName)
        {
            int column = reader.GetOrdinal(columnName);
            return reader.IsDBNull(column) ? (DateTime?)null : reader.GetDateTime(column);
        }

        public static Guid GetGuid(this IDataReader reader, string columnName)
        {
            int column = reader.GetOrdinal(columnName);
            return reader.IsDBNull(column) ? Guid.Empty : reader.GetGuid(column);
        }

        public static int? GetInt32(this IDataReader reader, string columnName)
        {
            int column = reader.GetOrdinal(columnName);
            return reader.IsDBNull(column) ? (int?)null : reader.GetInt32(column);
        }

        public static long? GetInt64(this IDataReader reader, string columnName)
        {
            int column = reader.GetOrdinal(columnName);
            return reader.IsDBNull(column) ? (long?)null : reader.GetInt64(column);
        }

        public static string GetString(this IDataReader reader, string columnName)
        {
            int column = reader.GetOrdinal(columnName);
            return reader.IsDBNull(column) ? null : reader.GetString(column);
        }        
    }
}
