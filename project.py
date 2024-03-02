"""
    This code implements a stop n go protocol for handling packet loss
    over a TCP connection. The TCP connection is simulated by provided
    files.
"""

from statistics import mean
import socket
import io
import time
import struct
import pandas as pd
import util
import util.logging

# Constants for packet type
DATA_PACKET = 0
ACK_P = 1
queue = []


def send(sock: socket.socket, data: bytes):
    """
    Implementation of the sending logic for sending data over a slow,
    lossy, constrained network using Stop-and-Wait ARQ.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.
        data -- A bytes object, containing the data to send over the network.
    """
    logger = util.logging.get_logger("project-sender")
    offsets = range(0, len(data), util.MAX_PACKET - 4)
    sock.settimeout(.05)
    sequence_number = 0

    try:
        for chunk in [data[i:i + util.MAX_PACKET] for i in offsets]:
            packet = struct.pack("!HH", DATA_PACKET, sequence_number) + chunk
            sock.send(packet)
            logger.info("Sent packet with sequence number %d", sequence_number)

            # Wait for ACK
            start_time = time.time()
            while True:
                try:
                    ack = sock.recv(util.MAX_PACKET)
                    ack_type, ack_sequence = struct.unpack("!HH", ack)

                    if len(queue) > 5:
                        queue.clear()

                    if ack_type == ACK_P and ack_sequence == sequence_number:
                        elapsed_time = time.time() - start_time
                        queue.append(elapsed_time)
                        rtt_series = pd.Series(queue)

                        moving_averages = round(rtt_series.ewm(
                            alpha=.3, adjust=True
                        ).mean(), 5)
                        sock.settimeout(mean(moving_averages.tolist()))
                        logger.info(
                            "Received ACK for sequence number %d", sequence_number)  # noqa
                        break
                except socket.timeout:
                    elapsed_time = time.time() - start_time
                    queue.append(elapsed_time)
                    rtt_series = pd.Series(queue)

                    moving_averages = round(rtt_series.ewm(
                        alpha=1, adjust=True
                    ).mean(), 5)
                    sock.settimeout(mean(moving_averages.tolist()))
                    sock.send(packet)
                    start_time = time.time()
            sequence_number = 1 - sequence_number
    except socket.error as e:
        logger.error("Socket error occurred: %s", str(e))


def recv(sock: socket.socket, dest: io.BufferedIOBase) -> int:
    """
    Implementation of the receiving logic for receiving data over a slow,
    lossy, constrained network using Stop-and-Wait ARQ.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.

    Return:
        The number of bytes written to the destination.
    """
    logger = util.logging.get_logger("project-receiver")
    num_bytes = 0
    expected_sequence_number = 0

    while True:
        try:
            packet = sock.recv(util.MAX_PACKET)
            if not packet:
                break

            packet_type, sequence_number = struct.unpack("!HH", packet[:4])

            if packet_type == DATA_PACKET and sequence_number == expected_sequence_number:  # noqa
                logger.info(
                    "Received packet with sequence number %d", sequence_number)
                dest.write(packet[4:])
                dest.flush()
                num_bytes += len(packet) - 4
                # Send ACK
                ack = struct.pack("!HH", ACK_P, sequence_number)
                sock.send(ack)
                logger.info("Sent ACK for sequence number %d", sequence_number)

                expected_sequence_number = 1 - expected_sequence_number
            elif packet_type == ACK_P:
                # Ignore duplicate ACKs
                logger.info(
                    "Received duplicate ACK for sequence number %d", sequence_number)  # noqa
            elif sequence_number != expected_sequence_number:
                logger.info("duplicate packet found")
                ack = struct.pack("!HH", ACK_P, sequence_number)
                sock.send(ack)
                logger.info("Sent ACK for sequence number %d", sequence_number)
            else:
                # Handle packet loss or corruption
                logger.info(
                    "Received out-of-sequence packet or corrupted packet. Ignoring.")  # noqa
        except socket.timeout:
            logger.info("Timed out durring reception")
        except socket.error as e:
            logger.error("Socket error occurred: %s", str(e))
            break

    return num_bytes
