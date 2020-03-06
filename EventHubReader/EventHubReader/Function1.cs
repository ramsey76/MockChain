using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Documents;

namespace EventHubReader
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run([EventHubTrigger("mockrock", Connection = "mockRockConnection", ConsumerGroup = "function")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();
            var cloudStorageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=mockchain;AccountKey=QwvHUFFk7yyMMbSKuPNfWMgpRCLVQAtfJjdz5Atkp1wDa+h/9OHJYZBoV/STGC4venCTM97KTB/sPiajrSH8Hw==;EndpointSuffix=core.windows.net");
            var tableClient = cloudStorageAccount.CreateCloudTableClient(new TableClientConfiguration());

            var table = tableClient.GetTableReference("mockchain1");
            var batchOperation = new TableBatchOperation();

            foreach (EventData eventData in events)
            {
                try
                {
                    var id = eventData.Properties["id"].ToString();
                    var transactionId = eventData.Properties["transActionId"].ToString();
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    var newRocMessage =  new RocMessage(transactionId, id, messageBody);

                    var insertOperation = TableOperation.Insert(newRocMessage);
                    batchOperation.Add(insertOperation);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function processed a message: {transactionId} - {id}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            await table.ExecuteBatchAsync(batchOperation);

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
