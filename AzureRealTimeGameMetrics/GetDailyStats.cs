using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace AzureRealTimeGameMetrics
{
    public static class GetDailyStats
    {
        [FunctionName("GetDailyStats")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "stats")]HttpRequestMessage req, TraceWriter log)
        {
            var redis = RedisHelper.Default;
            var dau = redis.GetDAU();
            var payingUsers = redis.GetPayingUsersDAU();
            var revenue = redis.GetRevenue();
            var installSources = redis.GetInstallSources();
            var dauPerInstallSource = installSources
                 .Select(i => new KeyValuePair<string, Task<long>>(
                    i, 
                    redis.GetInstallSourceDAU(i)
                ))
                .ToDictionary(i => i.Key, i => i.Value);
            await Task.WhenAll(
                dau,
                payingUsers,
                revenue
            );
            await Task.WhenAll(dauPerInstallSource.Values);

            var result = new
            {
                dau = dau.Result,
                payingUsersDau = payingUsers.Result,
                revenue = revenue.Result,
                arpu = revenue.Result / dau.Result,
                arppu = revenue.Result / payingUsers.Result,
                dauPerInstallSource = dauPerInstallSource.ToDictionary(i => i.Key, i => i.Value.Result)
            };
            return req.CreateResponse(HttpStatusCode.OK, result, "application/json");

        }
    }
}
