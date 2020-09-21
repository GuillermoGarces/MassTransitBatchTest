using System;
using System.Threading;

namespace MassTransitTest.Misc
{
    public static class ConcurrencyCounter<T>
    {
        public static int CurrentConsumerCount;
        public static int MaxConsumerCount;

        public static IDisposable Measure()
        {
            var current = Interlocked.Increment(ref CurrentConsumerCount);
            var maxConsumerCount = MaxConsumerCount;
            if (current > maxConsumerCount)
                Interlocked.CompareExchange(ref MaxConsumerCount, current, maxConsumerCount);

            return new DisposableAction(() => Interlocked.Decrement(ref CurrentConsumerCount));
        }

        class DisposableAction : IDisposable
        {
            private readonly Action action;

            public DisposableAction(Action action)
            {
                this.action = action;
            }
            
            public void Dispose()
            {
                action();
            }
        }
    }
}