using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Kaleidoscope.Benchmark
{
    public static class Program
    {
        public const int Concurrency = 40;
        public const int TimeToRun = 10; //in seconds

        //data to send (60 bytes)
        public static readonly Type DataMsgType = typeof(byte[]);
        public const int DataMsgSize = 250; // bytes
        public static readonly byte[] DataMsg = new byte[DataMsgSize];

        //number of messages to send
#if DEBUG
        public const int MsgToSend = 1 * Concurrency * 1000;
#else
        public const int MsgToSend = 100 * Concurrency * 1000;
#endif

        public volatile static Int32 MsgsReceived;
        public static long FirstReceiveTimestamp;

        static async Task Main(string[] args)
        {
            //args
            var test_case = args != null && args.Length > 1 ? int.Parse(args[0]) : 0;
            var enableP2P = true;
            var exchangeType = "direct";
            var brokerIP = "localhost";

            //select case
            if (args == null || args.Length == 0)
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
                await Testers.Tester_Kaleidoscope.InitTestbed(brokerIP, enableP2P);
            else if (test_case == 1)
                await Testers.Tester_RabbitMQ.InitTestbed(brokerIP, exchangeType);

            //Test loop
            GC.Collect();
            await Task.Delay(1000);
            while (true)
            {
                Console.WriteLine("");
                Console.WriteLine("Press any key to start test...");
                try { Console.ReadKey(); } catch { await Task.Delay(5_000); }
                Console.WriteLine("");

                Console.WriteLine("Starting test...");
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

        static async Task RunTest_MessageFlooding(Func<int, Task> runner)
        {
            //declares
            var stopwatch = new Stopwatch();
            var producerTasks = new Task[Concurrency];
            var toSend = MsgToSend / producerTasks.Length;

            Console.Write($"RunTest_MessageFlooding (concurrency:{Concurrency}) -> ");

            //start
            var starttimestamp = DateTime.UtcNow.Ticks;
            MsgsReceived = 0;
            stopwatch.Restart();
            for (int prod = 0; prod < producerTasks.Length; prod++)
                producerTasks[prod] = Task.Run(() => runner(toSend));

            //wait producers
            await Task.WhenAll(producerTasks);

            //wait for finish
            while (MsgsReceived < MsgToSend)
                await Task.Delay(100); //spin delay
            stopwatch.Stop();

            //wait to check for any duplicates
            await Task.Delay(1000);
            if (MsgsReceived != MsgToSend)
                Console.WriteLine($"**** warning, messages were lost or duplicated! the diff is MsgToSend-MsgsReceived={(MsgToSend - MsgsReceived)}");

            //compute latency
            var latency = (FirstReceiveTimestamp - starttimestamp) / TimeSpan.TicksPerMillisecond;

            //show results
            Console.WriteLine($"duration: {stopwatch.Elapsed.TotalSeconds:.00}sec , throughput: {((MsgsReceived / stopwatch.Elapsed.TotalSeconds) / 1000):.00}K msg/sec ,  latency: {latency}ms");
        }
    }
}
