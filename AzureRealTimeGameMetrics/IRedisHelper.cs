using System.Collections.Generic;
using System.Threading.Tasks;

namespace AzureRealTimeGameMetrics
{
    public interface IRedisHelper
    {
        Task RegisterUid(string uid);

        Task<long> GetInstallSourceDAU(string installSource);

        IEnumerable<string> GetInstallSources();
        Task<long> GetDAU();

        Task<long> GetPayingUsersDAU();

        Task<float> GetRevenue();

        Task RegisterInstallSource(string uid, string installSource);

        Task RegisterPayment(string uid, float paymentAmount);

        Task ResetStats();
    }
}