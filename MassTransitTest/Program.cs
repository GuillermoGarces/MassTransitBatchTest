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
using MassTransit.RabbitMqTransport;
using MassTransit.Configuration;

namespace MassTransitTest
{
    class Program
    {
        private static readonly int ProcessesCount = 2;
        public static readonly int MessagesCountPerProcess = 100;
        public static readonly TimeSpan ConsumersDelay = TimeSpan.FromMilliseconds(1);

        static async Task Main(string[] args)
        {
            File.Delete("batch.log");
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                .MinimumLevel.Override("MassTransit", LogEventLevel.Debug)
                .Enrich.FromLogContext()
                .WriteTo.Console(outputTemplate:
                    "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {messageId} {Message:lj}{NewLine}{Exception}")
                .WriteTo.File("batch.log",
                    outputTemplate:
                    "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {messageId} {Message:lj}{NewLine}{Exception}")
                .CreateLogger();

            AppContext.SetSwitch(AppContextSwitches.UseInternalCache, true);

            var provider = ConfigureServiceProvider();
            LogContext.ConfigureCurrentLogContext(provider.GetRequiredService<ILoggerFactory>());

            var counter = provider.GetRequiredService<MessageCounter2>();

            var bus = provider.GetRequiredService<IBusControl>();

            var result = bus.GetProbeResult();
            await File.WriteAllTextAsync("bus.json", result.ToJsonString());

            try
            {
                await bus.StartAsync();

                var messages = Enumerable.Range(1, ProcessesCount)
                    .Select(x => new InitProcess { Id = x.ToString("n0") })
                    .ToArray();
                await bus.SendConcurrently(messages, ProcessesCount);

                Console.ReadKey();

                counter.LogMissings(ProcessesCount, MessagesCountPerProcess);

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
                //x.Heartbeat(10);
            });

            cfg.PrefetchCount = 10;

            cfg.UseMessageRetry(r => r.Exponential(4, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(500)));
            cfg.UseLogging();

            cfg.ConfigureEndpoints(context);

            cfg.UseInMemoryOutbox(c => c.ConcurrentMessageDelivery = true);
        }
    }
}