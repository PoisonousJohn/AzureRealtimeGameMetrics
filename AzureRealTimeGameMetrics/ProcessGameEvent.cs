using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.ServiceBus;
using System.Diagnostics;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Configuration;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System.Globalization;

namespace AzureRealTimeGameMetrics
{
    public static class ProcessGameEvent
    {
        [FunctionName("ProcessGameEvent")]
        public static async Task Run(
            [EventHubTrigger(
                "game-events",
                Connection = "EventHub")]
            string[] msgs, ILogger log)
        {
            log.LogInformation($"Got {msgs.Length} msgs for processing");
            var timer = Stopwatch.StartNew();
            var tasks = msgs.Select(m => Task.Factory.StartNew(() => JsonConvert.DeserializeObject(m)));
            await Task.WhenAll(tasks);

            log.LogInformation($"{msgs.Length} msgs deserialized");

            var msgObjs = tasks.Select(t => t.Result);

            await ProcessMessages(_redis, msgObjs, log);

            log.LogInformation($"Function finished in {timer.ElapsedMilliseconds}ms");
        }

        public static async Task ProcessMessages(IRedisHelper redis, IEnumerable<object> msgs, ILogger log)
        {
            var redisTasks = new List<Task>();

            foreach (dynamic msg in msgs)
            {
                string uid = msg.uid?.ToString();
                if (string.IsNullOrEmpty(uid))
                {
                    log.LogError("Empty uid found. Ingoring");
                    continue;
                }

                redisTasks.Add(redis.RegisterUid(uid));

                string installSource = msg.install_source?.ToString();
                if (!string.IsNullOrEmpty(installSource))
                {
                    redisTasks.Add(redis.RegisterInstallSource(uid, installSource));
                }

                string type = msg.type?.ToString();
                if (type == "payment" && !string.IsNullOrEmpty(uid))
                {
                    NumberStyles style = NumberStyles.AllowDecimalPoint;
                    float paymentAmount = 0.0f;
                    float.TryParse(msg.amount?.ToString(), style, CultureInfo.InvariantCulture, out paymentAmount);
                    if (paymentAmount > 0.0f)
                    {
                        redisTasks.Add(redis.RegisterPayment(uid, paymentAmount));
                    }
                }
            }

            await Task.WhenAll(redisTasks);
        }

        private static IRedisHelper _redis { get { return RedisHelper.Default; } }

    }
}
