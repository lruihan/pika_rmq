import time
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello', durable=True)
'''
Round-Robin, assign to many workers
'''
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    #with ack
    ch.basic_ack(delivery_tag = method.delivery_tag)

#Fair dispatching
channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='hello')

print(' [*] Waiting for message. To exit press CTRL+C')
channel.start_consuming()