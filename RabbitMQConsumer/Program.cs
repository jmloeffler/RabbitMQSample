using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;

namespace RabbitMQConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var workTimeRandomizer = new Random();
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare("task_queue", true, false, false, null);

                    channel.BasicQos(0, 1, false);
                    var consumer = new QueueingBasicConsumer(channel);
                    channel.BasicConsume("task_queue", false, consumer);

                    Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");
                    while (true)
                    {
                        var ea = consumer.Queue.Dequeue();

                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" [x] Received {0}", message);

                        int workTime = workTimeRandomizer.Next(0, 2);
                        Thread.Sleep(workTime * 1000);

                        Console.WriteLine(" [x] Done");

                        if (ea.BasicProperties.IsReplyToPresent())
                        {
                            var replyProps = channel.CreateBasicProperties();
                            replyProps.CorrelationId = ea.BasicProperties.CorrelationId;
                            channel.BasicPublish("", ea.BasicProperties.ReplyTo, replyProps, body);
                        }

                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                }
            }
        }
    }
}
