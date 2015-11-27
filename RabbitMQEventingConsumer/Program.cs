using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQEventingConsumer
{
    class Program
    {
        private static readonly Random WorkTimeRandomizer = new Random();

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            {
                using (var model = connection.CreateModel())
                {
                    model.QueueDeclare("task_queue", true, false, false, null);

                    model.BasicQos(0, 1, false);
                    var consumer = new EventingBasicConsumer(model);

                    consumer.Received += Consumer_Received;

                    model.BasicConsume("task_queue", false, consumer);

                    Console.WriteLine(" [*] Waiting for messages. To exit press Enter");
                    Console.ReadLine();
                }
            }
        }

        private static void Consumer_Received(object sender, BasicDeliverEventArgs ea)
        {
            var body = ea.Body;
            var consumer = (EventingBasicConsumer) sender;
            var model = consumer.Model;
            var message = Encoding.UTF8.GetString(body);
            Console.WriteLine(" [x] Received {0}", message);

            int workTime = WorkTimeRandomizer.Next(0, 2);
            Thread.Sleep(workTime * 1000);

            Console.WriteLine(" [x] Done");

            if (ea.BasicProperties.IsReplyToPresent())
            {
                var replyProps = model.CreateBasicProperties();
                replyProps.CorrelationId = ea.BasicProperties.CorrelationId;
                model.BasicPublish("", ea.BasicProperties.ReplyTo, replyProps, body);
            }

            model.BasicAck(ea.DeliveryTag, false);
        }
    }
}
