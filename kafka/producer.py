from confluent_kafka import Producer
import pickle


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


if __name__ == '__main__':
    p = Producer({'bootstrap.servers': 'localhost'})

    # Corresponds to
    # static const unsigned int channelNumber = 13;
    # static const unsigned int crateId = 0;
    # static const unsigned int ts_high = 26001;
    # static const unsigned int ts_low = 123456789;
    # static const unsigned int slotId = 2;
    # static const unsigned int energy = 2345;

    datablock = [540717, 123456789, 26001, 2345]

    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce('test', pickle.dumps(datablock), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()
