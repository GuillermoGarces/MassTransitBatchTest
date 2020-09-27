using MassTransit;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace MassTransitTest
{
    public class InitProcess
    {
        public string Key { get; set; }
    }

    public class InitProcessConsumer : IConsumer<InitProcess>
    {
        private readonly ILogger<InitProcessConsumer> logger;
        private readonly MessageCounter2 counter;

        public InitProcessConsumer(ILogger<InitProcessConsumer> logger, MessageCounter2 counter)
        {
            this.logger = logger;
            this.counter = counter;
        }

        public async Task Consume(ConsumeContext<InitProcess> context)
        {
            for (var i = 0; i < Program.WorkCountPerProcess; i++)
            {
                await context.Send(new DoWork { Key = context.Message.Key + "-" + i });
            }

            counter.Consumed("InitProcess", new[] { context.Message.Key });
        }
    }
}