using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;


namespace SouthBayDotnet
{
    class ProgramFull
    {
        static Dictionary<string, object> producerConfig = new Dictionary<string, object>
        {
            { "bootstrap.servers", "192.168.43.9:9092" },
            { "api.version.request", "true" }
        };

        static Dictionary<string, object> consumerConfig = new Dictionary<string, object>
        {
            { "bootstrap.servers", "192.168.43.9:9092" },
            { "api.version.request", "true" },
            { "enable.auto.commit", false },
            { "group.id", "crawler" },
            { "session.timeout.ms", 6000 }, // note: newbie gotcha
            { "default.topic.config", new Dictionary<string, object>()
                {
                    { "auto.offset.reset", "smallest" }
                }
            }
        };

        static void Seed()
        {
            using (var producer = new Producer<string, Null>(producerConfig, new StringSerializer(Encoding.UTF8), null))
            {
                var deliveryReport = producer.ProduceAsync("url-queue", "https://news.ycombinator.com", null).Result;
                Console.WriteLine($"Wrote to partition: {deliveryReport.Partition}");
                producer.Flush();
            }
        }

        static void Run(string consumerName)
        {
            consumerConfig["client.id"] = consumerName;
            var seenUrls = new BloomFilter.Filter(10000);

            using (var webClient = new WebClient())
            using (var urlQueueConsumer = new Consumer<String, Null>(consumerConfig, new StringDeserializer(Encoding.UTF8), null))
            using (var producer = new Producer(producerConfig))
            {
                var urlQueueProducer = producer.GetSerializingProducer<string, Null>(new StringSerializer(Encoding.UTF8), null);
                var pageProducer = producer.GetSerializingProducer(new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8));

                urlQueueConsumer.Subscribe("url-queue");

                urlQueueConsumer.OnMessage += (_, msg) =>
                {
                    string url = msg.Key;

                    Console.WriteLine($"Retrieving: {url}");
                    string page = null;
                    try { page = webClient.DownloadString(url); }
                    catch (WebException) { }

                    if (page == null)
                    {
                        return;
                    }

                    var parsedPage = page.Substring(0, page.Length < 10000 ? page.Length : 10000);
                    pageProducer.ProduceAsync("pages", url, parsedPage);

                    var urls = new Regex("href\\s*=\\s*[\"'](http[@\\w\\s%#\\/\\.\\+;=\\?&:_-]*)[\"']")
                        .Matches(page)
                        .Cast<Match>()
                        .Select(g => g.Groups[1].Value);

                    var count = 0;
                    foreach (var u in urls)
                    {
                        if (!seenUrls.Contains(u))
                        {
                            // TODO: robots.txt + custom partitioner that colocates urls from same domain.
                            urlQueueProducer.ProduceAsync("url-queue", u, null);
                            seenUrls.Add(u);
                            count += 1;
                        }
                    }
                    Console.WriteLine($"Produced {count} url's to queue");

                    if (msg.Offset % 1 == 0)
                    {
                        urlQueueConsumer.CommitAsync();
                        Console.WriteLine($"Committed offset {msg.Offset} to topic/partition {msg.Topic}/{msg.Partition}");
                    }

                    // artificially slow down.
                    Thread.Sleep(TimeSpan.FromSeconds(5));
                };

                urlQueueConsumer.OnPartitionEOF += (_, tpo) =>
                    Console.WriteLine("reached end of " + tpo.TopicPartition);

                urlQueueConsumer.OnPartitionsAssigned += (_, ps) =>
                {
                    Console.WriteLine($"Assigned: [{string.Join(", ", ps.Select(p => p.Partition.ToString()).ToArray())}]");
                    urlQueueConsumer.Assign(ps);
                };

                urlQueueConsumer.OnPartitionsRevoked += (_, ps) =>
                {
                    Console.WriteLine($"Revoked: [{string.Join(", ", ps.Select(p => p.Partition.ToString()).ToArray())}]");
                    urlQueueConsumer.Unassign();
                };

                while (true)
                {
                    urlQueueConsumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }
        }

        static void MainFull(string[] args)
        {
            Run(args[0]);
            // Seed();
        }
    }
}