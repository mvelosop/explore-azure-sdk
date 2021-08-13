using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Rest;
using Microsoft.Rest.Serialization;
using Microsoft.Azure.Management.ResourceManager;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System.IO;
using Microsoft.Extensions.Configuration;
using DataFactorySamples.Settings;
using System.Diagnostics;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DataFactorySamples
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Data Factory Samples");

            var basePath = Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "../../.."));
            var configuration = new ConfigurationBuilder()
                .SetBasePath(basePath)
                .AddJsonFile("appsettings.json")
                .AddUserSecrets<Program>()
                .Build();

            var tenantId = configuration["tenantId"];
            var applicationId = configuration["applicationId"];
            var authenticationKey = configuration["authenticationKey"];
            var subscriptionId = configuration["subscriptionId"];

            // Authenticate and create a data factory management client
            var context = new AuthenticationContext("https://login.microsoftonline.com/" + tenantId);
            var clientCredential = new ClientCredential(applicationId, authenticationKey);
            var result = context.AcquireTokenAsync("https://management.azure.com/", clientCredential).Result;
            var tokenCredential = new TokenCredentials(result.AccessToken);

            var client = new DataFactoryManagementClient(tokenCredential) {
                SubscriptionId = subscriptionId
            };

            var settings = new TriggerSettings();
            configuration.GetSection("TriggerSettings")
                .Bind(settings);

            var response = await client.Triggers.GetWithHttpMessagesAsync(settings.ResourceGroupName, settings.FactoryName, settings.TriggerName);
            var triggerResource = response.Body;
            var trigger = triggerResource.Properties as ScheduleTrigger;
            var additionalProperties = JToken.FromObject(triggerResource.Properties.AdditionalProperties);

            Console.WriteLine("----- Trigger Resource:");
            Console.WriteLine(JsonConvert.SerializeObject(triggerResource, Formatting.Indented));
            Console.WriteLine("----- Properties:");
            Console.WriteLine(JsonConvert.SerializeObject(additionalProperties, Formatting.Indented));
            Console.WriteLine($"----- StartTime: {trigger.Recurrence.StartTime}");

            trigger.Recurrence.StartTime = trigger.Recurrence.StartTime.HasValue && trigger.Recurrence.StartTime < DateTime.UtcNow.Date
                ? DateTime.UtcNow.Date.AddDays(10)
                : DateTime.UtcNow.Date.AddDays(5);

            Console.WriteLine("----- UPDATED Trigger Resource:");
            Console.WriteLine(JsonConvert.SerializeObject(triggerResource, Formatting.Indented));

            var stopResponse = await client.Triggers.BeginStopWithHttpMessagesAsync(settings.ResourceGroupName, settings.FactoryName, settings.TriggerName);

            var stoppedTriggerResponse = await client.Triggers.GetWithHttpMessagesAsync(settings.ResourceGroupName, settings.FactoryName, settings.TriggerName);

            Console.WriteLine("----- STOPPED Trigger Resource:");
            Console.WriteLine(JsonConvert.SerializeObject(stoppedTriggerResponse.Body, Formatting.Indented));

            var updateResponse = await client.Triggers.CreateOrUpdateWithHttpMessagesAsync(settings.ResourceGroupName, settings.FactoryName, settings.TriggerName, triggerResource, "*");
            var startResponse = await client.Triggers.BeginStartWithHttpMessagesAsync(settings.ResourceGroupName, settings.FactoryName, settings.TriggerName);

            Console.WriteLine("----- RESPONSE Trigger Resource:");
            Console.WriteLine(JsonConvert.SerializeObject(updateResponse.Body, Formatting.Indented));

            Debugger.Break();
        }
    }
}
