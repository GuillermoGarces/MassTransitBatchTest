using MassTransit;
using MassTransit.ConsumeConfigurators;
using MassTransit.Definition;
using MassTransit.RabbitMqTransport;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace MassTransitTest
{
    public class DoWork
    {
        public Guid WorkProcessId { get; set; }
    }

    public class DoWorkConsumerDefinition : ConsumerDefinition<DoWorkConsumer>
    {
        protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator,
            IConsumerConfigurator<DoWorkConsumer> consumerConfigurator)
        {
            ((IRabbitMqReceiveEndpointConfigurator)endpointConfigurator).PrefetchCount = 1200;
            consumerConfigurator.Options<BatchOptions>(b =>
            {
                b.MessageLimit = 300;
                b.TimeLimit = TimeSpan.FromMilliseconds(200);
                b.ConcurrencyLimit = 10;
            });
        }
    }

    public class DoWorkConsumer : IConsumer<Batch<DoWork>>
    {
        private readonly ILogger<DoWorkConsumer> logger;
        private readonly MessageCounter2 counter;

        public DoWorkConsumer(ILogger<DoWorkConsumer> logger, MessageCounter2 counter)
        {
            this.logger = logger;
            this.counter = counter;
        }

        public async Task Consume(ConsumeContext<Batch<DoWork>> context)
        {
            await Task.Delay(Program.ConsumersDelay);

            foreach (var msg in context.Message)
            {
                await context.Send(new DoSomeExtraWork { WorkProcessId = msg.Message.WorkProcessId });
            }

            counter.Consumed("DoWork", context.Message.Select(x => x.Message.WorkProcessId).ToArray());
        }
    }
}