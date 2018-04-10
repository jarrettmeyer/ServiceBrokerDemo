# SQL Service Broker Demo

This is a demo project, showing how to work with SQL Service Broker from within a .NET project.

## Usage

Open SQL Management Studio and enter the following command.

```sql
EXEC [dbo].[SendTextMessage] @textMessage = 'Hello World!';
GO
```

This will drop a new message on a queue.
