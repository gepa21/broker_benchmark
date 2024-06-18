using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Benchmark
{
    public static class Program
    {
        public const int ProducerChannelCount = 1;
        public const int ConsumerChannelCount = 1;
        public const int Concurrency = 60;

        //data to send (60 bytes)
        public static readonly Type DataMsgType = typeof(byte[]);
        public const int DataMsgSize = 1_000; // bytes
        public static readonly byte[] DataMsg = new byte[DataMsgSize];

        //number of messages to send
#if DEBUG
        public const int MsgToSend = Concurrency * 100;
#else
        public const int MsgToSend = 50 * Concurrency * 1000;
#endif

        public static TestComponentModes TestComponentMode = TestComponentModes.Producer | TestComponentModes.Consumer;

        public volatile static Int32 MsgsReceived;
        public static long FirstReceiveTimestamp;
        public static long LastReceiveTimestamp;
        static TaskCompletionSource signal_start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        static TaskCompletionSource signal_end = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);


        static async Task Main(string[] args)
        {
            //args
            var test_case = args != null && args.Length > 1 ? int.Parse(args[0]) : 0;
            var enableP2P = true;
            var exchangeType = "direct";
            var brokerIP = "localhost";
            var encryption = false;
            args = new[] { "remote" };

            //extensive setup
            if (args != null && args.Length > 0 && args[0] == "remote")
                try
                {
                    //select broker
                    Console.WriteLine("Broker Host/IP :");
                    brokerIP = Console.ReadLine()?.Trim();
                    if (string.IsNullOrWhiteSpace(brokerIP))
                        brokerIP = "localhost";
                    encryption = brokerIP == "localhost" ? false : true;
                    Console.WriteLine("");

                    //select component mode
                    {
                        Console.WriteLine("Component modes.");
                        Console.WriteLine("   0 - Both (Producer & Consumer)");
                        Console.WriteLine("   1 - Producer");
                        Console.WriteLine("   2 - Consumer");
                        Console.Write("Select mode: ");
                        var input = Console.ReadLine();
                        Console.WriteLine("");
                        if (int.TryParse(input, out var inpInt))
                            TestComponentMode = inpInt switch
                            {
                                0 => TestComponentModes.Producer | TestComponentModes.Consumer,
                                1 => TestComponentModes.Producer,
                                2 => TestComponentModes.Consumer,
                            };
                        else
                            Console.Write($"err: unknown input. selecting TestComponentMode={TestComponentMode}");
                    }
                }
                catch { }

            //basic setup
            try
            {
                //select broker
                Console.WriteLine("Available brokers to test.");
                Console.WriteLine("   0 - Kaleidoscope");
                Console.WriteLine("   1 - RabbitMQ");
                Console.Write("Select broker: ");
                test_case = int.Parse(Console.ReadLine());
                Console.WriteLine("");

                //extra options
                if (test_case == 0)
                {
                    //select p2p
                    Console.WriteLine("P2P modes for Kaleidoscope.");
                    Console.WriteLine("   0 - Disabled");
                    Console.WriteLine("   1 - Enabled");
                    Console.Write("Select p2p mode : ");
                    var input = Console.ReadLine();
                    if (int.TryParse(input, out var inpInt))
                        enableP2P = inpInt == 0 ? false : true;
                    else if (bool.TryParse(input, out var inpBool))
                        enableP2P = inpBool;
                    else
                        Console.Write($"err: unknown input. selecting p2p={enableP2P}");
                }
                else if (test_case == 1)
                {
                    //select exchange type
                    Console.WriteLine("Exchange types.");
                    Console.WriteLine("   0 - Topic");
                    Console.WriteLine("   1 - Direct");
                    Console.Write("Select exchange type: ");
                    var input = Console.ReadLine().Trim();
                    if (string.Equals(input, "topic", StringComparison.OrdinalIgnoreCase))
                        exchangeType = "topic";
                    else if (string.Equals(input, "direct", StringComparison.OrdinalIgnoreCase))
                        exchangeType = "direct";
                    else if (int.TryParse(input, out var inpInt))
                        exchangeType = inpInt == 0 ? "topic" : "direct";
                    else
                        Console.Write($"err: unknown input. selecting exchangeType={exchangeType}");
                }
                Console.WriteLine("");
            }
            catch (Exception ex) { Console.WriteLine("Invalid input. " + ex.Message); return; }

            //init data msg
            new Random().NextBytes(DataMsg);

            //-------------------
            // Init Testbed
            //-------------------
            if (test_case == 0)
                await Testers.Tester_Kaleidoscope.InitTestbed(brokerIP, enableP2P, encryption, ConsumerChannelCount, ProducerChannelCount);
            else if (test_case == 1)
                await Testers.Tester_RabbitMQ.InitTestbed(brokerIP, exchangeType, encryption, ConsumerChannelCount, ProducerChannelCount);

            //wait warmup completion for consumer-only case
            if (TestComponentMode == TestComponentModes.Consumer)
            {
                Console.WriteLine("");
                Console.WriteLine("Waiting for producer warmup.");
                Console.WriteLine("Press any key to start test...");
                try { Console.ReadKey(); } catch { await Task.Delay(5_000); }
                Console.WriteLine("Starting test.");
                Console.WriteLine("");
            }

            //Test loop
            GC.Collect();
            await Task.Delay(2000);
            while (true)
            {
                if (TestComponentMode.HasFlag(TestComponentModes.Producer))
                {
                    Console.WriteLine("");
                    Console.WriteLine("Press any key to start test...");
                    try { Console.ReadKey(); } catch { await Task.Delay(5_000); }
                    Console.WriteLine("");
                    Console.WriteLine("Starting test...");
                }
                Console.WriteLine("");

                //-------------------
                // Tun test case
                //-------------------
                if (test_case == 0)
                    await RunTest_MessageFlooding(Testers.Tester_Kaleidoscope.RunTest_MessageFlooding);
                else if (test_case == 1)
                    await RunTest_MessageFlooding(Testers.Tester_RabbitMQ.RunTest_MessageFlooding);

                //finished
                Console.WriteLine("");
                Console.WriteLine("");
                GC.Collect();
                await Task.Delay(100);
            }
        }

        static async Task RunTest_MessageFlooding(Func<int, int, Task> runner)
        {
            //declares
            long starttimestamp;
            var stopwatch = new Stopwatch();

            //setup signals
            signal_start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            signal_end = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            //Start producer or consumer
            if (TestComponentMode.HasFlag(TestComponentModes.Producer))
            {
                Console.Write($"RunTest_MessageFlooding (concurrency:{Concurrency}) -> ");

                //start producers
                var producerTasks = new Task[Concurrency];
                var toSend = MsgToSend / producerTasks.Length;
                starttimestamp = DateTime.UtcNow.Ticks;
                MsgsReceived = 0;
                stopwatch.Restart();
                var channelInd = 0;
                for (int prod = 0; prod < producerTasks.Length; prod++)
                {
                    var channel = channelInd;
                    //start task
                    producerTasks[prod] = Task.Run(() => runner(channel, toSend));
                    //channel rr
                    channelInd++;
                    if (channelInd >= ProducerChannelCount)
                        channelInd = 0;
                }

                //wait producers
                await Task.WhenAll(producerTasks);
            }
            else
            {
                Console.WriteLine($"consumer reset and waiting...   ");
                Console.Write($"RunTest_MessageFlooding (concurrency:{Concurrency})");

                //wait for first-message to arrive to start consumer
                MsgsReceived = 0;
                await signal_start.Task;
                Console.Write($" -> ");
                starttimestamp = DateTime.UtcNow.Ticks;
                stopwatch.Restart();
            }

            //Test completion
            if (TestComponentMode == TestComponentModes.Producer)
            {
                Console.WriteLine($"Producers completed.");
            }
            else
            {
                //wait for finish
                await signal_end.Task;
                stopwatch.Stop();

                //wait to check for any duplicates
                await Task.Delay(2000);
                if (MsgsReceived != MsgToSend)
                    Console.WriteLine($"**** warning, messages were lost or duplicated! the diff is MsgToSend-MsgsReceived={(MsgToSend - MsgsReceived)}");

                //compute latency
                var latency = (FirstReceiveTimestamp - starttimestamp) / TimeSpan.TicksPerMillisecond;

                //show results
                Console.WriteLine($"duration: {stopwatch.Elapsed.TotalSeconds:.00}sec , throughput: {((MsgsReceived / stopwatch.Elapsed.TotalSeconds) / 1000):.00}K msg/sec ,  latency: {latency}ms");
            }
        }

        public static void RxHandler()
        {
            //count!
            var cnt = Interlocked.Increment(ref MsgsReceived);

            //keep timestamps and signal
            if (cnt == 1)
            {
                FirstReceiveTimestamp = DateTime.UtcNow.Ticks;
                signal_start.TrySetResult();
            }
            else if (cnt == MsgToSend)
            {
                LastReceiveTimestamp = DateTime.UtcNow.Ticks;
                signal_end.TrySetResult();
            }
        }
    }
}
