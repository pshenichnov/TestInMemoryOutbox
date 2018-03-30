using GreenPipes;
using MassTransit;
using MassTransit.RabbitMqTransport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using System;
using System.Threading.Tasks;

namespace Receiver
{
    class Program
    {
        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                    .AddJsonFile("appsettings.json", true, true);

            var configuration = builder.Build();

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Console()
                .ReadFrom.Configuration(configuration)
                .CreateLogger();

            Log.Information("Starting Receiver...");

            var services = new ServiceCollection();

            services.AddSingleton(context => Bus.Factory.CreateUsingRabbitMq(x =>
            {
                IRabbitMqHost host = x.Host(new Uri("rabbitmq://guest:guest@localhost:5672/test"), h => { });

                x.UseDelayedExchangeMessageScheduler();

                //x.UseInMemoryOutbox();

                x.ReceiveEndpoint(host, $"receiver_queue", e =>
                {
                    e.UseDelayedRedelivery(r =>
                    {
                        r.Interval(1, TimeSpan.FromMilliseconds(100));
                        r.Handle<Exception>();
                    });

                    e.UseMessageRetry(r =>
                    {
                        r.Interval(1, TimeSpan.FromMilliseconds(100));
                        r.Handle<Exception>();
                    });

                    x.UseInMemoryOutbox();

                    e.Consumer<TestHandler>();
                });

                x.UseSerilog();
            }));

            var container = services.BuildServiceProvider();

            var busControl = container.GetRequiredService<IBusControl>();

            busControl.Start();

            busControl.Publish<TestCommand>(new
            {
                Id = NewId.NextGuid()
            });

            Log.Information("Receiver started...");
        }
    }

    public interface TestCommand
    {
        Guid Id { get; }
    }

    public interface InnerCommand
    {
        Guid Id { get; }
    }

    public class TestHandler : IConsumer<TestCommand>, IConsumer<InnerCommand>
    {
        static int count = 0;

        public Task Consume(ConsumeContext<TestCommand> context)
        {
            context.Publish<InnerCommand>(new
            {
                Id = context.Message.Id
            });

            var redeliveryCount = context.Headers.Get<string>("MT-Redelivery-Count");

            Log.Information($"MT-Redelivery-Count: {redeliveryCount}; Total: {++count}");

            throw new Exception("something went wrong...");
        }

        public Task Consume(ConsumeContext<InnerCommand> context)
        {
            Log.Information($"Inner command: {context.Message.Id}");

            return Task.CompletedTask;
        }
    }
}