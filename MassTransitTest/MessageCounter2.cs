using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace MassTransitTest
{
    public class MessageCounter2
    {
        private readonly ILogger<MessageCounter2> logger;
        private readonly IDictionary<string, HashSet<string>> consumedKeys = new Dictionary<string, HashSet<string>>();

        public MessageCounter2(ILogger<MessageCounter2> logger)
        {
            this.logger = logger;
        }

        public void Consumed(string messageType, IEnumerable<string> receivedKeys)
        {
            lock (consumedKeys)
            {
                if (consumedKeys.TryGetValue(messageType, out var trackedKeys) == false)
                {
                    trackedKeys = new HashSet<string>();
                    consumedKeys.Add(messageType, trackedKeys);
                }

                foreach (var id in receivedKeys)
                {
                    trackedKeys.Add(id);
                }

                logger.LogInformation("      {0}", string.Join(" ", consumedKeys.Select(x => $"{x.Key} ({x.Value.Count})")));
            }
        }

        internal void LogMissingKeys()
        {
            var allIds = consumedKeys.SelectMany(x => x.Value).ToArray();

            var missed = new List<string>();
            for (var i = 0; i < Program.ProcessesCount; i++)
            {
                for (var j = 0; j < Program.WorkCountPerProcess; j++)
                {
                    var keyJ = $"{i}-{j}";

                    if (allIds.Contains(keyJ) == false) missed.Add(keyJ);

                    for (var k = 0; k < Program.ExtraWorkCountPerProcess; k++)
                    {
                        var keyK = $"{i}-{j}-{k}";

                        if (allIds.Contains(keyK) == false) missed.Add(keyK);
                    }
                }
            }

            if (missed.Any())
            {
                logger.LogError("Missing {0} messages: {1}", missed.Count, string.Join(", ", missed));
            }
            else
            {
                logger.LogInformation("No missed messages");
            }
        }
    }
}