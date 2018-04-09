using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SqlServiceBrokerUtil
{
    public class SqlServiceBrokerOperation
    {
        private SqlConnection _connection;
        private string _contract;
        private Guid _conversationHandle;
        private string _fromService;
        private readonly List<QueueMessage> _queueMessages = new List<QueueMessage>();
        private string _toService;
        private SqlTransaction _transaction;

        public IEnumerable<QueueMessage> QueueMessages
        {
            get { return _queueMessages; }
        }

        public SqlServiceBrokerOperation BeginDialog()
        {
            _conversationHandle = BrokerUtility.BeginDialog(_connection, _transaction, _fromService, _toService, _contract);
            return this;
        }

        public SqlServiceBrokerOperation BeginTransaction()
        {
            _transaction = _connection.BeginTransaction();
            return this;
        }

        public SqlServiceBrokerOperation ClearQueueMessages()
        {
            _queueMessages.Clear();
            return this;
        }

        public SqlServiceBrokerOperation CommitTransaction()
        {
            _transaction.Commit();
            return this;
        }        

        public SqlServiceBrokerOperation EndConversation()
        {
            BrokerUtility.EndConversation(_connection, _transaction, _conversationHandle);
            return this;
        }

        public SqlServiceBrokerOperation Receive(string queueName, TimeSpan? timeout = null, int? count = null)
        {
            if (timeout == null)
                timeout = TimeSpan.FromSeconds(10.0);

            if (count == null)
                count = 1;

            var messages = BrokerUtility.GetQueueMessages(_connection, _transaction, queueName, (TimeSpan)timeout, (int)count);
            _queueMessages.AddRange(messages);
            return this;
        }

        public SqlServiceBrokerOperation Send(string messageTypeName, byte[] bytes)
        {
            BrokerUtility.SendQueueMessage(_connection, _transaction, _conversationHandle, messageTypeName, bytes);
            return this;
        }

        public SqlServiceBrokerOperation UsingConnection(SqlConnection connection)
        {
            _connection = connection;
            return this;
        }

        public SqlServiceBrokerOperation UsingContract(string contract)
        {
            _contract = contract;
            return this;
        }

        public SqlServiceBrokerOperation UsingFromService(string fromService)
        {
            _fromService = fromService;
            return this;
        }

        public SqlServiceBrokerOperation UsingToService(string toService)
        {
            _toService = toService;
            return this;
        }
    }
}
