using SqlServiceBrokerUtil;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
                        var messageAsBodyAsString = BrokerUtility.Deserialize<string>(message.MessageBody);
                        Console.WriteLine(messageAsBodyAsString);
                    }
                }


            }
        }
    }
}
