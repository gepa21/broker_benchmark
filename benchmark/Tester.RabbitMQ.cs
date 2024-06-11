using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Reflection.Metadata.Ecma335;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace Kaleidoscope.Benchmark.Testers
{
    static class Tester_RabbitMQ
    {
        static ConnectionFactory connectionFactory;
        static IModel producerChannel;
        static IModel consumerChannel;

        const string exchName = "fiber.firefly.testexchange";
        const string queueName = "fiber.firefly.testexchange => testqueue";
        const string routingKey = "test_binding";

        const bool useSsl = false;

        public static async Task InitTestbed(string brokerIP, string exchangeType)
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
                    Enabled = useSsl,
                    Version = System.Security.Authentication.SslProtocols.Tls12,
                    CertPath = "client.crt",
                    CertificateValidationCallback = (_, _, _, _) => true,
                    ServerName = brokerIP     //this should match the name on the server certificate
                },
                UserName = "test",
                Password = "test",
                Port = useSsl ? 5671 : 5672
            };

            //create client
            var client1 = connectionFactory.CreateConnection();
            var client2 = connectionFactory.CreateConnection();

            //connect client 1
            Console.WriteLine("Connecting Client 1...");
            while (!client1.IsOpen) await Task.Delay(100);

            //connect client 2 
            Console.WriteLine("Connecting Client 2...");
            while (!client2.IsOpen) await Task.Delay(100);

            //get channels
            producerChannel = client1.CreateModel();
            consumerChannel = client2.CreateModel();

            //bind queue/exchange using routing key
            producerChannel.ExchangeDeclare(exchName, exchangeType, durable: false, autoDelete: true);
            consumerChannel.QueueDeclare(queueName, false, false, true, null);
            consumerChannel.QueueBind(queueName, exchName, routingKey);

            //create queue for consumer
            var consumerQueue = new CustomBasicConsumer(consumerChannel);
            consumerChannel.BasicConsume(queueName, true, consumerQueue);
            consumerChannel.BasicQos(0, 0, false);

            //warmup
            producerChannel.BasicPublish(exchName, routingKey, body: Program.DataMsg);
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
                //count!
                var cnt = Interlocked.Increment(ref Program.MsgsReceived);

                //keep timestamps
                if (cnt == 1)
                    Program.FirstReceiveTimestamp = DateTime.UtcNow.Ticks;
            }
        }


        public static async Task RunTest_MessageFlooding(int msgToSend)
        {
            for (int n = 0; n < msgToSend; n++)
                producerChannel.BasicPublish(exchName, routingKey, body: Program.DataMsg);
        }

    }
}
