import pika
import os

# Set up connection to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the queue (ensure it exists)
channel.queue_declare(queue='test-queue')

# Specify the full file path to avoid permission errors
file_path = 'consumer_output.txt'

# Counter to track the number of received messages
received_count = 0
expected_message_count = 10000  # We expect to receive 10,000 messages

# Open a file to log received messages
with open(file_path, 'w') as f:

    # Callback function to handle message consumption
    def callback(ch, method, properties, body):
        global received_count
        received_count += 1
        message = body.decode()
        print(f"Received: {message}")
        f.write(f"Received: {message}\n")

        # Manually acknowledge the message to RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # Check if all messages have been received
        if received_count == expected_message_count:
            print(f"All {received_count} messages received successfully. No messages lost.")
            f.write(f"All {received_count} messages received successfully. No messages lost.\n")
            f.flush()

    # Set up basic consumption (manual ack)
    channel.basic_consume(queue='test-queue', on_message_callback=callback, auto_ack=False)

    print('Waiting for messages...')
    f.write('Waiting for messages...\n')
    f.flush()

    # Start consuming messages
    channel.start_consuming()
