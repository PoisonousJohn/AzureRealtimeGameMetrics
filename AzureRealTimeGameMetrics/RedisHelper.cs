using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace AzureRealTimeGameMetrics
{
    public class RedisHelper : IRedisHelper
    {
        public static IRedisHelper Default {  get { return _default.Value;  } }

        private static Lazy<IRedisHelper> _default = new Lazy<IRedisHelper>(
            () =>
            {
                var connString = ConfigurationManager   
                                    .ConnectionStrings["Redis"]
                                    .ConnectionString;
                var redis = ConnectionMultiplexer.Connect(connString);
                return new RedisHelper(redis.GetDatabase());
            }
        );


        public RedisHelper(IDatabase redis)
        {
            _redis = redis;
        }

        public async Task RegisterUidAsync(string uid)
        {
            await _redis.SetAddAsync(UIDS_KEY, uid);
        }

        public Task<long> GetInstallSourceDAUAsync(string installSource)
        {
            return _redis.SetLengthAsync(installSource);
        }

        public IEnumerable<string> GetInstallSources()
        {
            return _redis.SetScan(INSTALL_SOURCES_KEY).Select(i => i.ToString());
        }
        public async Task<long> GetDAUAsync()
        {
            return await _redis.SetLengthAsync(UIDS_KEY);
        }

        public async Task<long> GetPayingUsersDAUAsync()
        {
            return await _redis.SetLengthAsync(PAYING_UIDS_KEY);
        }

        public async Task<float> GetRevenueAsync()
        {
            var revenue = await _redis.StringGetAsync(REVENUE_KEY);
            var stringRevenue = revenue.ToString();
            if (string.IsNullOrEmpty(stringRevenue))
            {
                return 0.0f;
            }
            float result = 0.0f;
            NumberStyles style = NumberStyles.AllowDecimalPoint;
            if (!float.TryParse(stringRevenue, style, CultureInfo.InvariantCulture, out result))
            {
                throw new Exception($"Failed to parse float string {stringRevenue}");
            }
            return result;
        }

        public async Task RegisterInstallSourceAsync(string uid, string installSource)
        {
            await Task.WhenAll(
                    _redis.SetAddAsync(INSTALL_SOURCES_KEY, installSource),
                    _redis.SetAddAsync(installSource, uid)
            );
        }

        public async Task RegisterPaymentAsync(string uid, float paymentAmount)
        {
            if (paymentAmount > 0.0f)
            {
                await Task.WhenAll(
                    _redis.SetAddAsync(PAYING_UIDS_KEY, uid),
                    _redis.StringIncrementAsync(REVENUE_KEY, paymentAmount)
                );
            }
        }

        public Task ResetStatsAsync()
        {
            var tasks = new List<Task>();
            foreach (var source in GetInstallSources())
            {
                tasks.Add(_redis.KeyDeleteAsync(source));
            }

            tasks.Add(_redis.KeyDeleteAsync(UIDS_KEY));
            tasks.Add(_redis.KeyDeleteAsync(REVENUE_KEY));
            tasks.Add(_redis.KeyDeleteAsync(PAYING_UIDS_KEY));
            tasks.Add(_redis.KeyDeleteAsync(INSTALL_SOURCES_KEY));

            return Task.WhenAll(tasks);
        }

            public async Task<DailyStats> GetDailyStatsAsync()
        {
            var redis = RedisHelper.Default;
            var dau = redis.GetDAUAsync();
            var payingUsers = redis.GetPayingUsersDAUAsync();
            var revenue = redis.GetRevenueAsync();
            var installSources = redis.GetInstallSources();
            var dauPerInstallSource = installSources
                 .Select(i => new KeyValuePair<string, Task<long>>(
                    i,
                    redis.GetInstallSourceDAUAsync(i)
                ))
                .ToDictionary(i => i.Key, i => i.Value);
            await Task.WhenAll(
                dau,
                payingUsers,
                revenue
            );
            await Task.WhenAll(dauPerInstallSource.Values);

            return new DailyStats
            {
                total = new TotalDailyStats
                {
                    time = DateTime.UtcNow,
                    dau = (int)dau.Result,
                    payingUsersDau = (int)payingUsers.Result,
                    revenue = revenue.Result,
                    arpu = dau.Result > 0 ? revenue.Result / dau.Result : 0.0f,
                    arppu = payingUsers.Result > 0 ? revenue.Result / payingUsers.Result : 0.0f,
                },
                dauPerInstallSource = dauPerInstallSource.ToDictionary(i => i.Key, i => (int)i.Value.Result)
            };

        }

        private readonly IDatabase _redis;

        private const string UIDS_KEY = "uids";
        private const string REVENUE_KEY = "revenue";
        private const string PAYING_UIDS_KEY = "payingUids";
        private const string INSTALL_SOURCES_KEY = "installSources";
    }
}
