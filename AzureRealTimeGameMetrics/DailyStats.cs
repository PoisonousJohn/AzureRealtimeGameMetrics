using System;
using System.Collections.Generic;

namespace AzureRealTimeGameMetrics
{
    public struct TotalDailyStats
    {
        public DateTime time { get; set; }
        public int dau { get; set; }
        public int payingUsersDau { get; set; }
        public float arpu { get; set; }
        public float arppu { get; set; }
        public float revenue { get; set; }

    }
    public struct DailyStats
    {
        public TotalDailyStats total { get; set; }
        public IDictionary<string, int> dauPerInstallSource { get; set; }
    }
}
