using System.Linq;
using System.Net.Http;
using System.Configuration;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System;

namespace AzureRealTimeGameMetrics
{
    public static class PushToPowerBI
    {
        [FunctionName("PushToPowerBI")]
        public static async void Run([TimerTrigger("*/15 * * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            var stats = await RedisHelper.Default.GetDailyStatsAsync();
            var totalStatsUrl = ConfigurationManager   
                                    .ConnectionStrings["TotalStatsUrl"]
                                    .ConnectionString;
            var perInstallSourceUrl = ConfigurationManager   
                                        .ConnectionStrings["PerInstallSourceUrl"]
                                        .ConnectionString;


            var perInstallSourceDau = stats.dauPerInstallSource
                .Select(i => new
                {
                    installSource = i.Key,
                    dau = i.Value,
                    time = DateTime.UtcNow
                }).ToArray();
            log.Info(JsonConvert.SerializeObject(perInstallSourceDau));
            var totalStatsTask = _http.PostAsJsonAsync(totalStatsUrl, new[] { stats.total });
            var perInstallSourceTask = _http.PostAsJsonAsync(
                                            perInstallSourceUrl,
                                            perInstallSourceDau
                                        );
            await Task.WhenAll(
                totalStatsTask,
                perInstallSourceTask
            );


            log.Info($"Total stats reported {totalStatsTask.Result.StatusCode}:{await totalStatsTask.Result.Content.ReadAsStringAsync()}");
            log.Info($"Per install source stats reported {perInstallSourceTask.Result.StatusCode}:{await perInstallSourceTask.Result.Content.ReadAsStringAsync()}");
        }

        private static readonly HttpClient _http = new HttpClient();
    }
}
