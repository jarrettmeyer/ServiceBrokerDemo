using SqlServiceBrokerUtil;
using System;
using System.Data.SqlClient;
using System.Text;

namespace ServiceBrokerDemo
{
    public class Program
    {
        public const string CONNECTION_STRING = "Data Source=(local);Initial Catalog=Broker_Demo;Integrated Security=True";
        public const string QUEUE_NAME = "[dbo].[TextMessageQueue]";

        public static void Main(string[] args)
        {
            bool done = false;

            while (!done)
            {
                using (var connection = new SqlConnection(CONNECTION_STRING))
                {
                    connection.Open();

                    var messages = new SqlServiceBrokerOperation()
                        .UsingConnection(connection)
                        .BeginTransaction()
                        .Receive(QUEUE_NAME)
                        .CommitTransaction()
                        .QueueMessages;

                    foreach (var message in messages)
                    {                        
                        var messageAsBodyAsString = Encoding.Unicode.GetString(message.MessageBody);
                        Console.WriteLine($"({message.MessageEnqueueTime}) - {messageAsBodyAsString}");                        
                    }
                }


            }
        }
    }
}
