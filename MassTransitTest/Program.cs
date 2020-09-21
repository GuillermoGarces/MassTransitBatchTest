using GreenPipes;
using MassTransit;
using MassTransit.Context;
using MassTransit.Definition;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using MassTransit.ConsumeConfigurators;
using MassTransit.RabbitMqTransport;

namespace MassTransitTest
{
    class Program
    {
        static async Task Main(string[] args)
        {
            File.Delete("batch.log");
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                .MinimumLevel.Override("MassTransit", LogEventLevel.Information)
                .Enrich.FromLogContext()
                .WriteTo.Console(outputTemplate:
                    "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {messageId} {Message:lj}{NewLine}{Exception}")
                .WriteTo.File("batch.log",
                    outputTemplate:
                    "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {messageId} {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            var provider = ConfigureServiceProvider();
            LogContext.ConfigureCurrentLogContext(provider.GetRequiredService<ILoggerFactory>());

            var bus = provider.GetRequiredService<IBusControl>();

            var result = bus.GetProbeResult();
            await File.WriteAllTextAsync("bus.json", result.ToJsonString());

            try
            {
                await bus.StartAsync();

                var messages = Enumerable.Range(0, 10).Select(_ => new InitProcess());
                await bus.SendConcurrently(messages, 10);

                await bus.Send(new InitProcess());

                Console.ReadKey();
            }
            finally
            {
                await bus.StopAsync();
            }
        }

        private static ServiceProvider ConfigureServiceProvider()
        {
            var services = new ServiceCollection();

            services.AddSingleton(typeof(MessageCounter2));

            services.AddLogging(b => b.SetMinimumLevel(LogLevel.Trace).AddSerilog());

            EndpointConvention.Map<InitProcess>(new Uri("queue:init-process"));
            EndpointConvention.Map<DoWork>(new Uri("queue:do-work"));
            EndpointConvention.Map<DoSomeExtraWork>(new Uri("queue:do-some-extra-work"));

            services.AddMassTransit(cfg =>
            {
                cfg.SetEndpointNameFormatter(KebabCaseEndpointNameFormatter.Instance);

                cfg.AddConsumers(Assembly.GetExecutingAssembly());

                cfg.UsingRabbitMq(ConfigureBus);
            });

            var provider = services.BuildServiceProvider();
            return provider;
        }

        private static void ConfigureBus(IBusRegistrationContext context, IRabbitMqBusFactoryConfigurator cfg)
        {
            cfg.Host("localhost", "radventure", x =>
            {
                x.Username("guest1");
                x.Password("guest1");
            });

            cfg.PrefetchCount = 10;

            cfg.UseMessageRetry(r => { r.Immediate(1); });
            cfg.UseLogging();

            cfg.ConfigureEndpoints(context);

            cfg.UseInMemoryOutbox(c => c.ConcurrentMessageDelivery = true);
        }
    }

    public class InitProcess
    {
    }

    public class DoWork
    {     
    }

    public class DoSomeExtraWork
    {
        public Guid InternalId { get; set; }
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

    public class DoSomeExtraWorkConsumerDefinition : ConsumerDefinition<DoSomeExtraWorkConsumer>
    {
        protected override void ConfigureConsumer(IReceiveEndpointConfigurator endpointConfigurator,
            IConsumerConfigurator<DoSomeExtraWorkConsumer> consumerConfigurator)
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
            //logger.LogInformation("Init process");

            //await Task.Delay(TimeSpan.FromMilliseconds(1000));

            var messages = Enumerable.Range(1, 5000).Select(_ => new DoWork());
            foreach (var msg in messages)
            {
                await context.Send(msg);
            }

            counter.Consumed("InitProcess", new[] { context.MessageId.Value });
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
            //logger.LogInformation("Consumed {0} DoWork: {1}", context.Message.Length, string.Join(",", context.Message.Select(x => x.MessageId).ToArray()));

            //await Task.Delay(TimeSpan.FromMilliseconds(1000));

            foreach (var msg in context.Message)
            {
                await context.Send(new DoSomeExtraWork { InternalId = msg.MessageId.Value });
            }

            counter.Consumed("DoWork", context.Message.Select(x => x.MessageId.Value).ToArray());
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
            //logger.LogInformation("Consumed {0} DoSomeExtraWork: {1}", context.Message.Length, string.Join(",", context.Message.Select(x => x.MessageId).ToArray()));

            //await Task.Delay(TimeSpan.FromMilliseconds(1000));

            counter.Consumed("DoSomeExtraWork", context.Message.Select(x => x.Message.InternalId).ToArray());
        }
    }
}