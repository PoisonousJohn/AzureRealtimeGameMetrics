using System.Collections.Generic;
using System.Threading.Tasks;

namespace AzureRealTimeGameMetrics
{
    public interface IRedisHelper
    {
        Task RegisterUidAsync(string uid);

        Task<long> GetInstallSourceDAUAsync(string installSource);

        IEnumerable<string> GetInstallSources();
        Task<long> GetDAUAsync();

        Task<long> GetPayingUsersDAUAsync();

        Task<float> GetRevenueAsync();

        Task RegisterInstallSourceAsync(string uid, string installSource);

        Task RegisterPaymentAsync(string uid, float paymentAmount);

        Task<DailyStats> GetDailyStatsAsync();

        Task ResetStatsAsync();
    }
}