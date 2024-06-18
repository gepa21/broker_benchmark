using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Reflection.Metadata.Ecma335;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace Benchmark.Testers
{
    static class Tester_RabbitMQ
    {
        static ConnectionFactory connectionFactory;
        static IModel[] producerChannels;
        static IModel[] consumerChannels;

        const string exchName = "fiber.firefly.testexchange";
        const string queueName = "fiber.firefly.testexchange => testqueue";
        const string routingKey = "test_binding";

        public static async Task InitTestbed(string brokerIP, string exchangeType, bool encryption, int consumerCount, int producerCount)
        {
            Console.WriteLine($"Initializing {nameof(Tester_RabbitMQ)}...");

            //setup connection factory
            connectionFactory = new ConnectionFactory()
            {
                HostName = brokerIP,
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(10),
                RequestedHeartbeat = TimeSpan.FromSeconds(40),
                Ssl = new SslOption
                {
                    Enabled = encryption,
                    Version = System.Security.Authentication.SslProtocols.Tls12,
                    CertPath = "client.crt",
                    CertificateValidationCallback = (_, _, _, _) => true,
                    ServerName = brokerIP     //this should match the name on the server certificate
                },
                UserName = "test",
                Password = "test",
                Port = encryption ? 5671 : 5672
            };

            //producer client
            producerChannels = new IModel[producerCount];
            if (Program.TestComponentMode.HasFlag(TestComponentModes.Producer))
                for (int n = 0; n < producerCount; n++)
                {
                    //create client
                    var producerClient = connectionFactory.CreateConnection();

                    //connect client
                    Console.WriteLine("Connecting producer client...");
                    while (!producerClient.IsOpen) await Task.Delay(100);

                    //get channel
                    var producerChannel = producerClient.CreateModel();
                    producerChannels[n] = producerChannel;

                    //setup exchange
                    producerChannel.ExchangeDeclare(exchName, exchangeType, durable: false, autoDelete: true);

                    //warmup
                    for (int i = 0; i < 100; i++)
                        producerChannel.BasicPublish(exchName, routingKey, body: Program.DataMsg);
                }

            //setup consumer queue
            consumerChannels = new IModel[consumerCount];
            if (Program.TestComponentMode.HasFlag(TestComponentModes.Consumer))
                for (int n = 0; n < consumerCount; n++)
                {
                    //create client
                    var consumerClient = connectionFactory.CreateConnection();

                    //connect client
                    Console.WriteLine("Connecting consumer client...");
                    while (!consumerClient.IsOpen) await Task.Delay(100);

                    //get channel
                    var consumerChannel = consumerClient.CreateModel();
                    consumerChannels[n] = consumerChannel;

                    //bind queue/exchange using routing key
                    consumerChannel.ExchangeDeclare(exchName, exchangeType, durable: false, autoDelete: true);
                    consumerChannel.QueueDeclare(queueName, false, false, true, null);
                    consumerChannel.QueueBind(queueName, exchName, routingKey);

                    //create queue for consumer
                    var consumerQueue = new CustomBasicConsumer(consumerChannel);
                    consumerChannel.BasicConsume(queueName, true, consumerQueue);
                    consumerChannel.BasicQos(0, 0, false);
                }
        }

        sealed class CustomBasicConsumer : DefaultBasicConsumer
        {
            readonly IModel Channel;

            public CustomBasicConsumer(IModel Channel)
                : base(Channel)
            {
                this.Channel = Channel;
            }

            public override void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, ReadOnlyMemory<byte> body)
            {
                //handle
                Program.RxHandler();
            }
        }


        public static async Task RunTest_MessageFlooding(int channel, int msgToSend)
        {
            var producerChannel = producerChannels[channel];
            for (int n = 0; n < msgToSend; n++)
                producerChannel.BasicPublish(exchName, routingKey, body: Program.DataMsg);
        }

    }
}
