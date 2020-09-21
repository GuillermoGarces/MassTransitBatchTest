using System;
using System.Collections.Generic;
using System.Linq;
using MassTransit;
using Microsoft.Extensions.Logging;

namespace MassTransitTest.Misc
{
    public static class DuplicatesDetector<T> where T : class
    {
        private static readonly HashSet<Guid?> alreadyReceived = new HashSet<Guid?>();

        public static bool AlreadyReceived(ILogger logger, Batch<T> batch)
        {
            lock (alreadyReceived)
            {
                if (batch.All(m => alreadyReceived.Contains(m.MessageId)))
                {
                    logger.LogError("Entire batch of {0} duplicated - completion mode was {1}", typeof(T).Name, batch.Mode);
                    return true;
                }

                if (batch.Any(m => alreadyReceived.Contains(m.MessageId)))
                {
                    throw new Exception($"Some {typeof(T).Name} messages duplicated (but not all)");
                }

                foreach (var msg in batch)
                {
                    alreadyReceived.Add(msg.MessageId);
                }

                return false;
            }
        }
    }
}