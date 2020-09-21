using MassTransit;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace MassTransitTest
{
    public class InitProcess
    {
        public Guid[] WorkProcessIds { get; set; }
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
            foreach (var id in context.Message.WorkProcessIds)
            {
                await context.Send(new DoWork { WorkProcessId = id });
            }

            counter.Consumed("InitProcess", new[] { context.MessageId.Value });
        }
    }
}