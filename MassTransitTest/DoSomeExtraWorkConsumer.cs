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
    public class DoSomeExtraWork
    {
        public string Id { get; set; }
    }

    public class DoSomeExtraWorkConsumerDefinition : ConsumerDefinition<DoSomeExtraWorkConsumer>
    {
        protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator,
            IConsumerConfigurator<DoSomeExtraWorkConsumer> consumerConfigurator)
        {
            ((IRabbitMqReceiveEndpointConfigurator)endpointConfigurator).PrefetchCount = 200;
            consumerConfigurator.Options<BatchOptions>(b =>
            {
                b.MessageLimit = 200;
                b.TimeLimit = TimeSpan.FromMilliseconds(200);
                b.ConcurrencyLimit = 10;
            });
        }
    }

    public class DoSomeExtraWorkConsumer : IConsumer<Batch<DoSomeExtraWork>>
    {
        private readonly ILogger<DoSomeExtraWorkConsumer> logger;
        private readonly MessageCounter2 counter;

        public DoSomeExtraWorkConsumer(ILogger<DoSomeExtraWorkConsumer> logger, MessageCounter2 counter)
        {
            this.logger = logger;
            this.counter = counter;
        }

        public async Task Consume(ConsumeContext<Batch<DoSomeExtraWork>> context)
        {
            await Task.Delay(Program.ConsumersDelay);
            counter.Consumed("DoSomeExtraWork", context.Message.Select(x => x.Message.Id).ToArray());
        }
    }
}