using System;
using System.Text;
using RabbitMQ.Client;

namespace RabbitMQProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare("task_queue", true, false, false, null);
                    var properties = channel.CreateBasicProperties();
                    properties.SetPersistent(true);

                    Console.WriteLine(" [*] Sending messages. " +
                                      "To exit press CTRL+C");
                    while (true)
                    {
                        var message = GetMessage();
                        var body = Encoding.UTF8.GetBytes(message);

                        channel.BasicPublish("", "task_queue", properties, body);
                        Console.WriteLine(" [x] Sent {0}", message);

                        System.Threading.Thread.Sleep(1000);
                    }
                }
            }
        }

        private static string GetMessage()
        {
            return Guid.NewGuid().ToString();
        }
    }
}
