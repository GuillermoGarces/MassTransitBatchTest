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

namespace MassTransitTest
{
    class Program
    {
        private static readonly int ProcessesCount = 20;
        private static readonly int MessagesCountPerProcess = 5000;
        public static readonly TimeSpan ConsumersDelay = TimeSpan.FromMilliseconds(200);

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

                var messages = Enumerable.Range(0, ProcessesCount)
                    .Select(_ => new InitProcess
                    {
                        WorkProcessIds = Enumerable.Range(0, MessagesCountPerProcess)
                            .Select(x => NewId.NextGuid())
                            .ToArray()
                    });
                await bus.SendConcurrently(messages, 5);

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
}