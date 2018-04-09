using System;
using System.Data;

namespace SqlServiceBrokerUtil
{
    public class QueueMessage
    {
        public QueueMessage(Guid conversationHandle, 
            string messageTypeName, 
            byte[] messageBody, 
            string serviceContractName = null, 
            string serviceName = null, 
            long? messageSequenceNumber = null,
            DateTime? messageEnqueueTime = null)
        {
            ConversationHandle = conversationHandle;
            MessageBody = messageBody;
            MessageEnqueueTime = messageEnqueueTime;
            MessageSequenceNumber = messageSequenceNumber;
            MessageTypeName = messageTypeName;
            ServiceContractName = serviceContractName;
            ServiceName = serviceName;
        }

        public Guid ConversationHandle { get; private set; }

        public byte[] MessageBody { get; private set; }

        public DateTime? MessageEnqueueTime { get; private set; }

        public long? MessageSequenceNumber { get; private set; }

        public string MessageTypeName { get; private set; }

        public string ServiceContractName { get; private set; }

        public string ServiceName { get; private set; }

        public static QueueMessage FromDataReader(IDataReader reader)
        {
            Guid conversationHandle = reader.GetGuid("conversation_handle");
            string messageTypeName = reader.GetString("message_type_name");
            byte[] messageBody = reader.GetBytes("message_body");
            string serviceContractName = reader.GetString("service_contract_name");
            string serviceName = reader.GetString("service_name");
            long? messageSequenceNumber = reader.GetInt64("message_sequence_number");
            DateTime? messageEnqueueTime = reader.GetDateTime("message_enqueue_time");

            var queueMessage = new QueueMessage(conversationHandle, messageTypeName, messageBody, 
                serviceContractName, serviceName, messageSequenceNumber, messageEnqueueTime);
            return queueMessage;
        }
    }
}
