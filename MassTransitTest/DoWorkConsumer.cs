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
        public string Id { get; set; }
    }

    public class DoWorkConsumerDefinition : ConsumerDefinition<DoWorkConsumer>
    {
        protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator,
            IConsumerConfigurator<DoWorkConsumer> consumerConfigurator)
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
                for (var i = 1; i <= Program.MessagesCountPerProcess; i++)
                {
                    await context.Send(new DoSomeExtraWork { Id = msg.Message.Id + "-" + i });
                }
            }

            counter.Consumed("DoWork", context.Message.Select(x => x.Message.Id).ToArray());
        }
    }
}