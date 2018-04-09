using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace SqlServiceBrokerUtil
{
    public static class BrokerUtility
    {
        public const string END_DIALOG_MESSAGE_TYPE = "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog";
        public const string ERROR_MESSAGE_TYPE = "http://schemas.microsoft.com/SQL/ServiceBroker/EndDialog";

        public static Guid BeginDialog(SqlConnection connection, SqlTransaction transaction, string fromService, string toService, string contract)
        {
            using (var command = connection.CreateCommand())
            {
                // Set the command text to begin a dialog.
                command.CommandText = $"BEGIN DIALOG @handle FROM SERVICE {fromService} TO SERVICE {toService} ON CONTRACT {contract} WITH ENCRYPTION = OFF";
                command.Transaction = transaction;

                // Set the @handle parameter.
                var handleParam = command.CreateParameter();
                handleParam.ParameterName = "@handle";
                handleParam.SqlDbType = SqlDbType.UniqueIdentifier;
                handleParam.Direction = ParameterDirection.Output;
                command.Parameters.Add(handleParam);

                // Execute the query.
                command.ExecuteNonQuery();

                // Get the value of the parameter.
                Guid handle = (Guid)handleParam.Value;
                return handle;
            }
        }

        public static object Deserialize(byte[] bytes)
        {
            if (bytes != null && bytes.LongLength > 0)
            {
                using (var stream = new MemoryStream(bytes))
                {
                    return Encoding.Unicode.GetString(bytes);
                    //var formatter = new BinaryFormatter();
                    //return formatter.Deserialize(stream);
                }
            }
            else
            {
                return null;
            }
        }

        public static T Deserialize<T>(byte[] bytes)
        {
            var obj = Deserialize(bytes);
            if (obj != null)
            {
                return (T)obj;
            }
            return default(T);
        }

        public static void EndConversation(SqlConnection connection, SqlTransaction transaction, Guid conversationHandle)
        {
            using (var command = new SqlCommand())
            {
                // Create the command.
                command.CommandText = "END CONVERSATION @handle";
                command.Transaction = transaction;

                // Add the @handle parameter.
                var handleParam = command.CreateParameter();
                handleParam.ParameterName = "@handle";
                handleParam.SqlDbType = SqlDbType.UniqueIdentifier;
                handleParam.Value = conversationHandle;
                command.Parameters.Add(handleParam);

                // Execute the command.
                command.ExecuteNonQuery();
            }
        }

        public static IEnumerable<QueueMessage> GetQueueMessages(SqlConnection connection, SqlTransaction transaction, string queueName, TimeSpan timeout, int count = 1)
        {
            var messages = new List<QueueMessage>();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"WAITFOR ( RECEIVE TOP({count}) * FROM {queueName} ), TIMEOUT {timeout.TotalMilliseconds}";
                command.Transaction = transaction;

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var message = QueueMessage.FromDataReader(reader);
                        messages.Add(message);
                    }
                }
            }

            return messages;
        }

        public static void SendQueueMessage(SqlConnection connection, SqlTransaction transaction, Guid handle, string messageTypeName, byte[] bytes)
        {
            using (var command = connection.CreateCommand())
            {
                // Create the SQL command to send a message.
                command.CommandText = $"SEND ON CONVERSATION @handle MESSAGE TYPE {messageTypeName} ( @messageBody )";
                command.Transaction = transaction;

                // Set the @handle parameter.
                var handleParam = command.CreateParameter();
                handleParam.ParameterName = "@handle";
                handleParam.SqlDbType = SqlDbType.UniqueIdentifier;
                handleParam.Value = handle;
                command.Parameters.Add(handleParam);

                // Set the @messageBody parameter.
                var messageBodyParam = command.CreateParameter();
                messageBodyParam.ParameterName = "@messageBody";
                messageBodyParam.SqlDbType = SqlDbType.VarBinary;
                messageBodyParam.Value = bytes;
                command.Parameters.Add(messageBodyParam);

                // Execute the SQL command.
                command.ExecuteNonQuery();
            }
        }

        public static byte[] Serialize(object obj)
        {
            using (var stream = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                formatter.Serialize(stream, obj);
                stream.Flush();
                return stream.ToArray();
            }
        }
    }
}
