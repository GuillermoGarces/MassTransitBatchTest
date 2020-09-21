using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Logging;

namespace MassTransitTest
{
    public class MessageCounter2
    {
        private readonly ILogger<MessageCounter2> logger;
        private readonly IDictionary<string, List<Guid>> consumedIds = new Dictionary<string, List<Guid>>();

        public MessageCounter2(ILogger<MessageCounter2> logger)
        {
            this.logger = logger;
        }

        public void Consumed(string key, Guid[] workProcessIds)
        {
            lock (consumedIds)
            {
                List<Guid> oldIds;
                if (consumedIds.TryGetValue(key, out oldIds) == false)
                {
                    oldIds = new List<Guid>();
                    consumedIds.Add(key, oldIds);
                }

                var newIds = workProcessIds.Where(x => oldIds.Contains(x) == false).ToArray();
                oldIds.AddRange(newIds);

                var sb = new StringBuilder();
                sb.Append("Consumed messages: ");
                foreach (var item in consumedIds)
                {                    
                    sb.AppendFormat(" - Type {0}: {1}", item.Key, item.Value.Count());
                }

                logger.LogInformation(sb.ToString());
            }
        }
    }
}