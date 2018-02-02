using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using System.Diagnostics;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Configuration;
using System.Collections.Generic;

namespace AzureRealTimeGameMetrics
{
    public static class ProcessGameEvent
    {
        [FunctionName("ProcessGameEvent")]
        public static async Task Run(
            [EventHubTrigger(
                "game-events",
                Connection = "EventHub")]
            string[] msgs, TraceWriter log)
        {
            log.Info($"Got {msgs.Length} msgs for processing");
            var timer = Stopwatch.StartNew();
            var tasks = msgs.Select(m => Task.Factory.StartNew(() => JsonConvert.DeserializeObject(m)));
            await Task.WhenAll(tasks);

            log.Info($"{msgs.Length} msgs deserialized");

            var msgObjs = tasks.Select(t => t.Result);

            var redisTasks = new List<Task>();

            foreach (dynamic msg in msgObjs)
            {
                string uid = msg.uid?.ToString();
                if (string.IsNullOrEmpty(uid))
                {
                    log.Error("Empty uid found. Ingoring");
                    continue;
                }

                redisTasks.Add(_redis.GetDatabase().SetAddAsync("uids", uid));

                string installSource = msg.install_source?.ToString();
                if (!string.IsNullOrEmpty(installSource))
                {
                    redisTasks.Add(_redis.GetDatabase().SetAddAsync("installSources", installSource));
                    redisTasks.Add(_redis.GetDatabase().SetAddAsync(installSource, uid));
                }
            }

            log.Info($"Function finished in {timer.ElapsedMilliseconds}ms");
        }

        private static ConnectionMultiplexer _redis { get { return _redisLazy.Value; } }

        private static Lazy<ConnectionMultiplexer> _redisLazy = new Lazy<ConnectionMultiplexer>(
            () =>
            {
                var connString = ConfigurationManager   
                                    .ConnectionStrings["Redis"]
                                    .ConnectionString;
                return ConnectionMultiplexer.Connect(connString);
            }
        );
    }
}
