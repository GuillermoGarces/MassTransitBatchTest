using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace MassTransitTest
{
    public class MessageCounter2
    {
        private readonly ILogger<MessageCounter2> logger;
        private readonly IDictionary<string, HashSet<string>> consumedIds = new Dictionary<string, HashSet<string>>();

        public MessageCounter2(ILogger<MessageCounter2> logger)
        {
            this.logger = logger;
        }

        public void Consumed(string key, IEnumerable<string> receivedIds)
        {
            lock (consumedIds)
            {
                if (consumedIds.TryGetValue(key, out var trackedIds) == false)
                {
                    trackedIds = new HashSet<string>();
                    consumedIds.Add(key, trackedIds);
                }

                foreach (var id in receivedIds)
                {
                    trackedIds.Add(id);
                }

                logger.LogInformation("      {0}", string.Join(" ", consumedIds.Select(x => $"{x.Key} ({x.Value.Count})")));
            }
        }

        internal void LogMissings(int processesCount, int messagesCountPerProcess)
        {
            var allIds = consumedIds.SelectMany(x => x.Value).ToArray();

            for (var i = 1; i <= processesCount; i++)
            {
                for (var j = 1; j <= messagesCountPerProcess; j++)
                {
                    var keyJ = string.Format("{0}-{1}", i, j);

                    if (allIds.Contains(keyJ) == false)
                        logger.LogWarning("Missing key: {0}", keyJ);

                    for (var k = 1; k <= messagesCountPerProcess; k++)
                    {
                        var keyK = string.Format("{0}-{1}-{2}", i, j, k);

                        if (allIds.Contains(keyK) == false)
                            logger.LogWarning("Missing key: {0}", keyK);
                    }
                }
            }
        }
    }
}