using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.Cosmos.Table;

namespace EventHubReader
{
    public class RocMessage : TableEntity
    {
        public string Message { get; set; }

        public RocMessage(string transActionId, string id, string body)
        {
            PartitionKey = transActionId;
            RowKey = id;
            this.Message = body;
        }
    }
}
