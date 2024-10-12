import pika

# Establish connection to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue (create if it doesn't exist)
channel.queue_declare(queue='test-queue')

# Number of messages to send
message_count = 10000

# Send messages
for i in range(1, message_count + 1):
    message = f'Message {i}'
    channel.basic_publish(exchange='',
                          routing_key='test-queue',
                          body=message)
    print(f"Sent: {message}")

# Close the connection
connection.close()
print(f"All {message_count} messages sent.")
