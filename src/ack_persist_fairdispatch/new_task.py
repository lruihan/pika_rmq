import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

message = ' '.join(sys.argv[1:]) or "Hello World!"

#make queue durable, can't change durable if already declared
channel.queue_declare(queue='hello', durable=True)
channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=message,
                      properties=pika.BasicProperties(
                          delivery_mode=2, #make message durable
                      ))
print(" [x] Sent %r" % message)
connection.close()