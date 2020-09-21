using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using GreenPipes.Util;

namespace MassTransitTest.Misc
{
    public class MessageCounter<T>
    {
        private readonly int messageCount;
        private readonly TaskCompletionSource<TimeSpan> completed = TaskUtil.GetTask<TimeSpan>();
        private readonly Stopwatch stopwatch;

        private int consumed;

        public MessageCounter()
        {
            //messageCount = Program.MessagesCount;
            stopwatch = Stopwatch.StartNew();
        }

        public Task<TimeSpan> Completed => completed.Task;

        public void Consumed(int n)
        {
            var c = Interlocked.Add(ref consumed, n);

            if (c == messageCount)
            {
                completed.TrySetResult(stopwatch.Elapsed);
            }
        }

        public async Task<double> GetRate() => consumed / (await Completed).TotalSeconds;
    }
}