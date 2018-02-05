using System.Linq;
using System.Collections.Generic;
using AzureRealTimeGameMetrics;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using StackExchange.Redis;
using System.Dynamic;
using System.Collections;

namespace AzureRealTimeGameMetricsTests
{
    [TestClass]
    public class RealTimeGameMetricsTests
    {
        [TestMethod]
        public void GameEventProcessingIntegrationTest()
        {
            var log = Mock.Of<ILogger>();
            var redis = GetRedis();
            redis.ResetStats();
            var msgs = new List<object>();

            var msg = new Dictionary<string, object>
            {
                {  "install_source", "unitTest" },
                {  "uid", "1" }
            };

            msgs.Add(msg.ToDynamic());

            ProcessGameEvent.ProcessMessages(redis, msgs, log).GetAwaiter().GetResult();
            Assert.AreEqual(1, redis.GetDAU().GetAwaiter().GetResult());
            Assert.AreEqual(1, redis.GetInstallSourceDAU("unitTest").GetAwaiter().GetResult());
            Assert.AreEqual(1, redis.GetInstallSources().Count());
            Assert.AreEqual("unitTest", redis.GetInstallSources().First());
            Assert.AreEqual(0, redis.GetPayingUsersDAU().GetAwaiter().GetResult());
            Assert.AreEqual(0, redis.GetRevenue().GetAwaiter().GetResult());

            msg.Add("type", "payment");
            msg.Add("amount", "0.1");

            msgs[0] = msg.ToDynamic();

            ProcessGameEvent.ProcessMessages(redis, msgs, log).GetAwaiter().GetResult();

            Assert.AreEqual(1, redis.GetDAU().GetAwaiter().GetResult());
            Assert.AreEqual(1, redis.GetInstallSourceDAU("unitTest").GetAwaiter().GetResult());
            Assert.AreEqual(1, redis.GetInstallSources().Count());
            Assert.AreEqual("unitTest", redis.GetInstallSources().First());
            Assert.AreEqual(1, redis.GetPayingUsersDAU().GetAwaiter().GetResult());
            var result = System.Math.Abs(redis.GetRevenue().GetAwaiter().GetResult() - 0.1);
            Assert.IsTrue(result < 0.0001);

            msg["uid"] = "2";
            msgs[0] = msg.ToDynamic();

            ProcessGameEvent.ProcessMessages(redis, msgs, log).GetAwaiter().GetResult();

            Assert.AreEqual(2, redis.GetDAU().GetAwaiter().GetResult());
            Assert.AreEqual(2, redis.GetInstallSourceDAU("unitTest").GetAwaiter().GetResult());
            Assert.AreEqual(1, redis.GetInstallSources().Count());
            Assert.AreEqual("unitTest", redis.GetInstallSources().First());
            Assert.AreEqual(2, redis.GetPayingUsersDAU().GetAwaiter().GetResult());
            result = System.Math.Abs(redis.GetRevenue().GetAwaiter().GetResult() - 0.2);
            Assert.IsTrue(result < 0.0001);

            msg["install_source"] = "unitTest2";
            msg["uid"] = "3";
            msg.Remove("type");
            msg.Remove("amount");
            msgs[0] = msg.ToDynamic();

            ProcessGameEvent.ProcessMessages(redis, msgs, log).GetAwaiter().GetResult();
            
            Assert.AreEqual(3, redis.GetDAU().GetAwaiter().GetResult());
            Assert.AreEqual(2, redis.GetInstallSourceDAU("unitTest").GetAwaiter().GetResult());
            Assert.AreEqual(1, redis.GetInstallSourceDAU("unitTest2").GetAwaiter().GetResult());
            Assert.AreEqual(2, redis.GetInstallSources().Count());
            Assert.AreEqual(2, redis.GetPayingUsersDAU().GetAwaiter().GetResult());
            result = System.Math.Abs(redis.GetRevenue().GetAwaiter().GetResult() - 0.2);
            Assert.IsTrue(result < 0.0001);
        }

        private IRedisHelper GetRedis()
        {
            var connStr = System.IO.File.ReadAllText("redis.txt");
            return new RedisHelper(ConnectionMultiplexer.Connect(connStr).GetDatabase());
        }

    }

    internal static class DictUtils
    {
        public class SafeExpandoObject : DynamicObject, IDictionary<string, object>
        {
            private readonly Dictionary<string, object> values
                = new Dictionary<string, object>();

            public object this[string key] { get => throw new System.NotImplementedException(); set => throw new System.NotImplementedException(); }

            public ICollection<string> Keys => values.Keys;

            public ICollection<object> Values => values.Values;

            public int Count => values.Count;

            public bool IsReadOnly => false;

            public void Add(string key, object value)
            {
                values.Add(key, value);
            }

            public void Add(KeyValuePair<string, object> item)
            {
                values.Add(item.Key, item.Value);
            }

            public void Clear()
            {
                values.Clear();
            }

            public bool Contains(KeyValuePair<string, object> item)
            {
                return values.Contains(item);
            }

            public bool ContainsKey(string key)
            {
                return values.ContainsKey(key);
            }

            public void CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
            {
                throw new System.NotImplementedException();
            }

            public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
            {
                return values.GetEnumerator();
            }

            public bool Remove(string key)
            {
                return values.Remove(key);
            }

            public bool Remove(KeyValuePair<string, object> item)
            {
                return values.Remove(item.Key);
            }

            public override bool TryGetMember(GetMemberBinder binder, out object result)
            {
                values.TryGetValue(binder.Name, out result);
                return true;
            }

            public bool TryGetValue(string key, out object value)
            {
                throw new System.NotImplementedException();
            }

            public override bool TrySetMember(SetMemberBinder binder, object value)
            {
                values[binder.Name] = value;
                return true;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                throw new System.NotImplementedException();
            }
        }

        public static dynamic ToDynamic(this Dictionary<string, object> dict)
        {
            return dict.Aggregate(new SafeExpandoObject() as IDictionary<string, object>,
                            (a, p) => { a.Add(p.Key, p.Value); return a; });

        }
    }
}
