import functools
import time

import click
import boto3
import numpy

from concurrent import futures


def send_message(client, message):

    start = time.time()
    client.put_record(
        DeliveryStreamName='test',
        Record={
            'Data': message
        }
    )
    end = time.time()
    return end - start


@click.command()
@click.option('--message_count', '-c', default=10000)
@click.option('--workers', '-w', default=1000)
@click.option('--message_size', '-s', default=1024)
def main(message_count, workers, message_size):
    client = boto3.client('firehose')

    message = b'0' * message_size

    messages = [message] * workers

    partial_send_message = functools.partial(send_message, client)

    all_times = []

    with futures.ThreadPoolExecutor(max_workers=int(workers)) as executor:

        messages_left = message_count

        while messages_left > 0:

            print '%d messages left...' % messages_left

            results = executor.map(partial_send_message, messages)

            all_times += list(results)

            messages_left -= workers

    print 'Total calls made: %f' % len(all_times)
    print 'Min: %f' % min(all_times)
    print 'Max: %f' % max(all_times)
    print 'Average: %f' % numpy.average(all_times)


if __name__ == '__main__':
    main()
