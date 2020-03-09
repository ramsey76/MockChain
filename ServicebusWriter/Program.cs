using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp.Serialization;
using Microsoft.Azure.ServiceBus;

namespace ServicebusWriter
{
    class Program
    {
        static IQueueClient queueClient;

        public static async Task Main(string[] args)
        {
            const string ServiceBusConnectionString = "Endpoint=sb://mockchainsb.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=NrmGCYWWgdob+HY5Jc9O1RoFDciq896eXxKOecRm8Hg=";
            const string QueueName = "vts";

            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

            await SendMessagesAsync(10000);

        }

        static async Task SendMessagesAsync(int numberOfMessagesToSend)
        {
            try
            {
                var messages = new List<Message>();
                var transactionId = Guid.NewGuid();
                var p = 1;

                var startTime = DateTime.Now;

                for (var i = 1; i <= numberOfMessagesToSend; i++)
                {
                    // Create a new message to send to the queue.
                    var content =
"{\"Id\":\"343\",\"LastName\":\"Geregistreed\",\"Prefix\":\"\",\"FirstName\":\" \",\"Initials\":\"LP\",\"Title\":\" \",\"Gender\":\"M\",\"DateOfBirth\":\"\",\"DateOfDeceased\":\"\",\"PartnerName\":\" \",\"PartnerPrefix\":\" \",\"PartnerInitials\":\" \",\"PreferenceName\":\"\",\"CocNumber\":\"\",\"DepartmentNumber\":\"272726\",\"PrivatePostStreet\":\"KNSM Laan\",\"PrivatePostHouseNumber\":\"300\",\"PrivatePostHouseNumberPrefix\":\"\",\"PrivatePostHouseNumberPostfix\":\"\",\"PrivatePostPostcode\":\"1782GR\",\"PrivatePostCity\":\"\",\"PrivatePostCountry\":\"\",\"PrivateVisitStreet\":\"Graan voor Visch\",\"PrivateVisitHouseNumber\":\"12\",\"PrivateVisitHouseNumberPrefix\":\"\",\"PrivateVisitHouseNumberPostfix\":\"\",\"PrivateVisitPostcode\":\"1789FG\",\"PrivateVisitCity\":\"Enkhuizen\",\"PrivateVisitCountry\":\"NL\",\"PrivateGeneralStreet\":\"Stadionplein\",\"PrivateGeneralHouseNumber\":\"286\",\"PrivateGeneralHouseNumberPrefix\":\"\",\"PrivateGeneralHouseNumberPostfix\":\"\",\"PrivateGeneralPostcode\":\"2678GR\",\"PrivateGeneralCity\":\"\",\"PrivateGeneralCountry\":\"\",\"BusinessPostStreet\":\"\",\"BusinessPostHouseNumber\":\"\",\"BusinessPostHouseNumberPrefix\":\"\",\"BusinessPostHouseNumberPostfix\":\"\",\"BusinessPostPostcode\":\"\",\"BusinessPostCity\":\"\",\"BusinessPostCountry\":\"\",\"BusinessVisitStreet\":\"\",\"BusinessVisitHouseNumber\":\"\",\"BusinessVisitHouseNumberPrefix\":\"\",\"BusinessVisitHouseNumberPostfix\":\"\",\"BusinessVisitPostcode\":\"\",\"BusinessVisitCity\":\"\",\"BusinessVisitCountry\":\"\",\"BusinessGeneralStreet\":\"\",\"BusinessGeneralHouseNumber\":\"90\",\"BusinessGeneralHouseNumberPrefix\":\"\",\"BusinessGeneralHouseNumberPostfix\":\"\",\"BusinessGeneralPostcode\":\"\",\"BusinessGeneralCity\":\"\",\"BusinessGeneralCountry\":\"\",\"BusinessEmail\":\"frankderood@gmail.com\",\"PrivateEmail\":\"frankpeetersamsterdam@gmail.com\",\"PrivatePhone\":\"0630554437\",\"PrivateMobile\":\"0620000000\",\"BusinessPhone\":\"0653425522\",\"BusinessMobile\":\"0625413242\",\"ExternalCorrelationId\":\"\",\"BranchSales\":\"\",\"BranchAftersales\":\"\",\"AlternativeId\":\"104\",\"PermissionCarRelatedEmail\":\"True\",\"PermissionCarRelatedPhone\":\"True\",\"PermissionCarRelatedMail\":\"True\",\"PermissionCarRelatedMessaging\":\"True\",\"PermissionCompanyRelatedEmail\":\"True\",\"PermissionCompanyRelatedPhone\":\"True\",\"PermissionCompanyRelatedMail\":\"True\",\"PermissionCompanyRelatedMessaging\":\"True\",\"PermissionHoldingRelatedEmail\":\"True\",\"PermissionHoldingRelatedPhone\":\"True\",\"PermissionHoldingRelatedMail\":\"True\",\"PermissionHoldingRelatedMessaging\":\"True\",\"PermissionGeneralCommercialEmail\":\"True\",\"PermissionGeneralCommercialPhone\":\"True\",\"PermissionGeneralCommercialMail\":\"True\",\"PermissionGeneralCommercialMessaging\":\"True\",\"PermissionBrandModelRelatedEmail\":\"True\",\"PermissionBrandModelRelatedPhone\":\"True\",\"PermissionBrandModelRelatedMail\":\"True\",\"PermissionBrandModelRelatedMessaging\":\"True\",\"PermissionSurveyEmail\":\"True\",\"PermissionSurveyPhone\":\"True\",\"PermissionSurveyMail\":\"True\",\"PermissionSurveyMessaging\":\"True\",\"RegistratieDatum\":\"\"}";
                    var message = new Message(Encoding.UTF8.GetBytes(content));
                    message.UserProperties.Add("id", i.ToString());
                    message.UserProperties.Add("transActionId", transactionId.ToString());


                    messages.Add(message);

                    // Write the body of the message to the console.
                    //Console.WriteLine($"Added message: {i} with Transaction: {transactionId.ToString()}");
                }
                Console.WriteLine("Created a list of messages");

                var pagesize = 25;
                var offset = 0;
                var resultSize = pagesize;
                while (resultSize == pagesize)
                {
                    if (offset + pagesize > messages.Count)
                    {
                        pagesize = messages.Count - offset;
                    }

                    var package = messages.GetRange(offset, pagesize);
                    resultSize = package.Count;
                    offset = offset + resultSize;
                    pagesize = 25;

                    await queueClient.SendAsync(package);
                    Console.WriteLine($"send package with: {offset} - {pagesize}");
                }

                
                Console.WriteLine($"{startTime} - {DateTime.Now.ToString()}");
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }
    }
}
