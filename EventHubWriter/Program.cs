using System;
using System.Collections.Generic;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Amqp.Serialization;

namespace EventHubWriter
{
    class Program
    {
        private static EventHubClient eventHubClient;
        private const string EventHubConnectionString = "Endpoint=sb://mockchain.servicebus.windows.net/;SharedAccessKeyName=manage;SharedAccessKey=jxgjx8yXPNHhsE3mKR+YDNWUm9EMttHQINnVkIFztmw=;EntityPath=mockrock";
        private const string EventHubName = "mockrock";

        static async Task Main(string[] args)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            await SendMessagesToEventHub(500000);

            await eventHubClient.CloseAsync();

        }

        private static async Task SendMessagesToEventHub(int numMessagesToSend)
        {
            var startTime = DateTime.Now;

            var content =
    "{\"Id\":\"343\",\"LastName\":\"Geregistreed\",\"Prefix\":\"\",\"FirstName\":\" \",\"Initials\":\"LP\",\"Title\":\" \",\"Gender\":\"M\",\"DateOfBirth\":\"\",\"DateOfDeceased\":\"\",\"PartnerName\":\" \",\"PartnerPrefix\":\" \",\"PartnerInitials\":\" \",\"PreferenceName\":\"\",\"CocNumber\":\"\",\"DepartmentNumber\":\"272726\",\"PrivatePostStreet\":\"KNSM Laan\",\"PrivatePostHouseNumber\":\"300\",\"PrivatePostHouseNumberPrefix\":\"\",\"PrivatePostHouseNumberPostfix\":\"\",\"PrivatePostPostcode\":\"1782GR\",\"PrivatePostCity\":\"\",\"PrivatePostCountry\":\"\",\"PrivateVisitStreet\":\"Graan voor Visch\",\"PrivateVisitHouseNumber\":\"12\",\"PrivateVisitHouseNumberPrefix\":\"\",\"PrivateVisitHouseNumberPostfix\":\"\",\"PrivateVisitPostcode\":\"1789FG\",\"PrivateVisitCity\":\"Enkhuizen\",\"PrivateVisitCountry\":\"NL\",\"PrivateGeneralStreet\":\"Stadionplein\",\"PrivateGeneralHouseNumber\":\"286\",\"PrivateGeneralHouseNumberPrefix\":\"\",\"PrivateGeneralHouseNumberPostfix\":\"\",\"PrivateGeneralPostcode\":\"2678GR\",\"PrivateGeneralCity\":\"\",\"PrivateGeneralCountry\":\"\",\"BusinessPostStreet\":\"\",\"BusinessPostHouseNumber\":\"\",\"BusinessPostHouseNumberPrefix\":\"\",\"BusinessPostHouseNumberPostfix\":\"\",\"BusinessPostPostcode\":\"\",\"BusinessPostCity\":\"\",\"BusinessPostCountry\":\"\",\"BusinessVisitStreet\":\"\",\"BusinessVisitHouseNumber\":\"\",\"BusinessVisitHouseNumberPrefix\":\"\",\"BusinessVisitHouseNumberPostfix\":\"\",\"BusinessVisitPostcode\":\"\",\"BusinessVisitCity\":\"\",\"BusinessVisitCountry\":\"\",\"BusinessGeneralStreet\":\"\",\"BusinessGeneralHouseNumber\":\"90\",\"BusinessGeneralHouseNumberPrefix\":\"\",\"BusinessGeneralHouseNumberPostfix\":\"\",\"BusinessGeneralPostcode\":\"\",\"BusinessGeneralCity\":\"\",\"BusinessGeneralCountry\":\"\",\"BusinessEmail\":\"frankderood@gmail.com\",\"PrivateEmail\":\"frankpeetersamsterdam@gmail.com\",\"PrivatePhone\":\"0630554437\",\"PrivateMobile\":\"0620000000\",\"BusinessPhone\":\"0653425522\",\"BusinessMobile\":\"0625413242\",\"ExternalCorrelationId\":\"\",\"BranchSales\":\"\",\"BranchAftersales\":\"\",\"AlternativeId\":\"104\",\"PermissionCarRelatedEmail\":\"True\",\"PermissionCarRelatedPhone\":\"True\",\"PermissionCarRelatedMail\":\"True\",\"PermissionCarRelatedMessaging\":\"True\",\"PermissionCompanyRelatedEmail\":\"True\",\"PermissionCompanyRelatedPhone\":\"True\",\"PermissionCompanyRelatedMail\":\"True\",\"PermissionCompanyRelatedMessaging\":\"True\",\"PermissionHoldingRelatedEmail\":\"True\",\"PermissionHoldingRelatedPhone\":\"True\",\"PermissionHoldingRelatedMail\":\"True\",\"PermissionHoldingRelatedMessaging\":\"True\",\"PermissionGeneralCommercialEmail\":\"True\",\"PermissionGeneralCommercialPhone\":\"True\",\"PermissionGeneralCommercialMail\":\"True\",\"PermissionGeneralCommercialMessaging\":\"True\",\"PermissionBrandModelRelatedEmail\":\"True\",\"PermissionBrandModelRelatedPhone\":\"True\",\"PermissionBrandModelRelatedMail\":\"True\",\"PermissionBrandModelRelatedMessaging\":\"True\",\"PermissionSurveyEmail\":\"True\",\"PermissionSurveyPhone\":\"True\",\"PermissionSurveyMail\":\"True\",\"PermissionSurveyMessaging\":\"True\",\"RegistratieDatum\":\"\"}";

            var transactionId = Guid.NewGuid();

            var p = 0;
            var eventDatas = new List<EventData>();


            for (var i = 0; i < numMessagesToSend; i++)
            {
                var message = $"Message {i}";
                Console.WriteLine($"Creating message: {message} with TranactionId: {transactionId}");
                var eventData = new EventData(Encoding.UTF8.GetBytes(content));
                eventData.Properties.Add("id", i.ToString());
                eventData.Properties.Add("transActionId", transactionId.ToString());
                eventDatas.Add(eventData);

                if (p == 50)
                {
                    try
                    {
                        await eventHubClient.SendAsync(eventDatas);
                        Console.WriteLine("send batch");
                    }
                    catch (Exception exception)
                    {
                        Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                    }

                    eventDatas.Clear();
                    p = 0;
                }
                p++;
            }

            var endTime = DateTime.Now;
            Console.WriteLine($"{numMessagesToSend} messages sent. StartTime: {startTime.ToString()}, EndTime: {endTime.ToString()} with TransactionId: {transactionId}");
        }
    }
}
