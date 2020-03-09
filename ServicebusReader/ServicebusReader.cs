using System;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace Company.Function
{
    public static class ServicebusReader
    {
        [FunctionName("ServicebusReader")]
        public static void Run([ServiceBusTrigger("vts", Connection = "ServiceBusConnection")]Message[] myQueueItem, ILogger log)
        {
            try
            {
                log.LogInformation($"Received a batch of messages, with size of: {myQueueItem.Length}");

                foreach (var message in myQueueItem)
                {
                    log.LogInformation(
                        $"C# ServiceBus queue trigger function processed message: {message.UserProperties["id"]}");
                }
            }
            catch (Exception exception)
            {
                log.LogError(exception.Message);
            }
        }
    }
}
