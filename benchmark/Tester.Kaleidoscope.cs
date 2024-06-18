using Phoesion;
using Phoesion.Glow.Kaleidoscope.Client;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Benchmark.Testers
{
    static class Tester_Kaleidoscope
    {
        static Producer[] producers;
        static BasicQueueConsumer[] consumerQueues;

        static string ClientAuthPrivKey = "Firefly:test_keys:mXgDnVzvoFzje7G0:VPFJYBgcmKHeHeiqjT+ODsfVhuAITQVC+0PjM+NYV8JSPZmECR37HSBa7sM1uVYoVrQ/NRibwUS2/s1j02F3q3wttbuCSPG1ibZqKpY85se31I8Hr47x/gROGCpLsl3G0XpQBk9VZkFinfVACG4xny4i1Xk/0nomhMVrWIakY4TQRVT/XJStSV3W1XgRGHhIkuuTGMJyMjLvMHwIvMfl+8KD4C1Og4FVVKnGDCdU/3MKyl+K/1Y0PJYg3Hxd5lccwprNWMiI3iERYuNaNhkw/tlx9q6Q9QGMUkScbriUYQSsE4RTPdMMS5qpQzwo60bXm1JvHhVXmV6jJbmhMKYwag/eq3+zaAlIWeXsX7Atg78ZpRUwAJ2Uk4puwtr6t9zXhL3LowmPqm+8pfxuAbyZeP8qPax2SCNkQ6RpUUh8X7W9yO9NaEAgK/fPA+2dsB7ce9FtFQaQaHYa6rF2xtX8vQ8t+t8n6kygCBAetBJlqh5+MQNawFWG1DUYTmga0E5djSxuNwr4FW1UOYxaFXySXL/rhQtuSWRQaWFjQcl68+uPgRrWyupwXl/dgnRqKlM7saLqQAzFk8eSaGzyQrt/xSV3x5BvKPaKU6nbjDuoIjfTYEz6b82ilMZK2DHb4/JeHhW4nKpWukfVsbn/B299fNr1OAww2niOW9R8k29Mk1k=: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:AQACAwAAAAEAAQACAADvJnsAMP1QSriXL5JNpQce2/pl3h55v1GQViDrTA7ecc+T8NTbb6B62TrfitffI1uw9uZ1I8yt6XmmVp1G3Olmc64TQKHJhk6WkjlkInCsEuLworxi1SLCP11xPOZgkizAF48jy77G/p0kqKcggVljOpwXWmqZgsaFIvZYxoH2P7wqh5BNUPgjLFIVSS95mFYxdd9UrsQ2Dlj74agINeNXpMfQDJr+e6/RP0O4zqybLOy6VtLGYbTl1EOSTpExD3MlwcQxvYapKQdvnZKx/RqNS8GKtDnGerKSbdtu5Nivada7sMvyLH9ji7PyT30jTHFwXztb+LPWkeJCQ0xFfcElJrTqVubjEHw9f1inJ/iJNmhp5YthznFLfR3F/ZITMbmmfGHs0chSj3EU+ZCiNbI0+FfLJARvo+77XpR8gkjVe6HZWu5RSMb7sBiVheKG30G0p4y8Eq63++OcYyRa4/XBEv5k/4yddmRCFHvHbUPlRm0Qlqwjbyz6izuVTZb8g1Dzj/CCT1C9xbxyySY6mM6omzGLtH0G6Ec5BuL0wTQhL7e70qik/dDvT99oXM48jJK3RbdQFWNZxEnqzxpVDltOQCsLRI6RgyhvmbqB1QqyCt6rs4PcbNhfJzWrc8aDVvu+sViG1fs9SsyfxKa/s1Cg2JjNUUGqb/uXFsi5CGu2JQ==:882709144";

        const string exchName = "fiber.firefly.testexchange";
        const string queueName = "fiber.firefly.testexchange => testqueue";
        const string routingKey = "test_binding";

        static readonly Type[] UserProtocol = new Type[] { typeof(byte[]) };


        public static async Task InitTestbed(string brokerIP, bool enableP2P, bool encryption, int consumerCount, int producerCount)
        {
            //register resolvers
            Phoesion.Glow.Base.MsgPack.Resolvers.RegisterResolvers();

            Console.WriteLine($"Initializing {nameof(Tester_Kaleidoscope)}...");

            //deserialize auth key
            var authKey = Phoesion.Glow.API.Internal.EntityAuthKey.Deserialize(encryption ? ClientAuthPrivKey : null);

            //connect consumer 
            consumerQueues = new BasicQueueConsumer[consumerCount];
            if (Program.TestComponentMode.HasFlag(TestComponentModes.Consumer))
                for (int n = 0; n < consumerCount; n++)
                {
                    //create client
                    var consumerClient = new ClientConnection("test", 0, UserProtocol, brokerIP, 15010, EntityId: "consumer" + n, EntityType: "Firefly", EnableP2P: enableP2P, ServerPublicKey: authKey?.AuthorityPublicKey, ClientAuthKeyId: authKey?.KeyId, ClientAuthSignature: authKey?.CreateConnectionSignature("consumer" + n, 0, ServiceName: "srv2"), ServiceName: "srv2");

                    Console.WriteLine("Connecting consumer client...");
                    var connectedCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    consumerClient.OnConnectionOpenEvent += _c => { Console.WriteLine("*** consumer client connection opened"); connectedCompletion.SetResult(); };
                    await consumerClient.StartConnection();
                    await connectedCompletion.Task;

                    //create queue for consumer
                    var consumerQueue = new BasicQueueConsumer(consumerClient, exchName, queueName, [routingKey], onRX, QueueType: QueueTypes.AsyncQueue);
                    consumerQueues[n] = consumerQueue;
                    await Task.Delay(250);
                }

            //connect producer
            producers = new Producer[producerCount];
            if (Program.TestComponentMode.HasFlag(TestComponentModes.Producer))
                for (int n = 0; n < producerCount; n++)
                {
                    //create client
                    var producerClient = new ClientConnection("test", 0, UserProtocol, brokerIP, 15010, EntityId: "producer" + n, EntityType: "Firefly", EnableP2P: enableP2P, ServerPublicKey: authKey?.AuthorityPublicKey, ClientAuthKeyId: authKey?.KeyId, ClientAuthSignature: authKey?.CreateConnectionSignature("producer" + n, 0, ServiceName: "srv1"), ServiceName: "srv1");

                    Console.WriteLine("Connecting producer client...");
                    var connectedCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    producerClient.OnConnectionOpenEvent += _c => { Console.WriteLine("*** producer client connection opened"); connectedCompletion.SetResult(); };
                    await producerClient.StartConnection();
                    await connectedCompletion.Task;

                    //create producer
                    var producer = new Producer(producerClient, exchName);
                    producers[n] = producer;

                    //warmup 
                    Console.WriteLine("Warming up");
                    for (int i = 0; i < 100; i++)
                        await producer.SendMessageAsync(Program.DataMsg, Program.DataMsgType, routingKey);
                    GC.Collect();
                    await Task.Delay(1000);

                    //wait for p2p
                    if (enableP2P)
                    {
                        Console.WriteLine("Waiting for p2p...");
                        while (producer.ClientConnection.P2PMesh.Peers.Count == 0)
                        {
                            await Task.Delay(2_000);
                            await producer.SendMessageAsync(Program.DataMsg, Program.DataMsgType, routingKey);
                        }
                        while (producer.ClientConnection.P2PMesh.Peers.Values.All(c => !c.IsOpen))
                        {
                            await Task.Delay(4_000);
                            await producer.SendMessageAsync(Program.DataMsg, Program.DataMsgType, routingKey);
                        }
                    }
                    for (int i = 0; i < 100; i++)
                        await producer.SendMessageAsync(Program.DataMsg, Program.DataMsgType, routingKey);
                }
        }

        static readonly RxHandlerResult noop = new RxHandlerResult(null);

        static async ValueTask<RxHandlerResult> onRX(BasicQueueConsumer consumer, Phoesion.Glow.Kaleidoscope.Messages.QueueMsg _msg, Type msgType, CancellationToken cancellationToken, IHandlerContext ctx)
        {
            //handle
            Program.RxHandler();

            //this are messages, so we don't need to return anything.
            return noop;
        }

        public static async Task RunTest_MessageFlooding(int channel, int msgToSend)
        {
            var producer = producers[channel];
            for (int n = 0; n < msgToSend; n++)
                await producer.SendMessageAsync(Program.DataMsg, Program.DataMsgType, routingKey).ConfigureAwait(false);
        }
    }
}
