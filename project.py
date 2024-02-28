import socket
import io
import time
import typing
import struct
import util
import util.logging

# Constants for packet type
DATA_PACKET = 0
ACK_PACKET = 1
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
    chunk_size = util.MAX_PACKET
    pause = .1
    offsets = range(0, len(data), util.MAX_PACKET - 4)
    sock.settimeout(.05)
    sequence_number = 0
    timeout = .1

    try:
        for chunk in [data[i:i + chunk_size] for i in offsets]:
            #print(struct.pack("!HH", DATA_PACKET, sequence_number))
            #print(chunk)
            packet = struct.pack("!HH", DATA_PACKET, sequence_number) + chunk
            sock.send(packet)
            logger.info("Sent packet with sequence number %d", sequence_number)

            # Wait for ACK
            start_time = time.time()
            while True:
                try:
                    ack = sock.recv(util.MAX_PACKET)
                    #print("in send while")
                    ack_type, ack_sequence = struct.unpack("!HH", ack)
                    if ack_type == ACK_PACKET and ack_sequence == sequence_number:
                        logger.info("Received ACK for sequence number %d", sequence_number)
                        print("\n ack received")
                        break
                except socket.timeout:
                    elapsed_time = time.time() - start_time
                    if elapsed_time >= timeout:
                        logger.error("Timeout waiting for ACK. Resending packet with sequence number %d", sequence_number)
                        sock.send(packet)
                        start_time = time.time()
                    else:
                        logger.debug("Still waiting for ACK for sequence number %d", sequence_number)
                    
            sequence_number = 1 - sequence_number

            logger.info("Pausing for %f seconds", round(pause, 2))
            time.sleep(pause)
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

            if packet_type == DATA_PACKET and sequence_number == expected_sequence_number:
                logger.info("Received packet with sequence number %d", sequence_number)
                dest.write(packet[4:])
                dest.flush()
                num_bytes += len(packet) - 4
                # Send ACK  
                ack = struct.pack("!HH", ACK_PACKET, sequence_number)
                sock.send(ack)
                logger.info("Sent ACK for sequence number %d", sequence_number)

                expected_sequence_number = 1 - expected_sequence_number
            elif packet_type == ACK_PACKET:
                # Ignore duplicate ACKs
                logger.info("Received duplicate ACK for sequence number %d", sequence_number)
            elif sequence_number != expected_sequence_number:
                logger.info("duplicate packet found")
                ack = struct.pack("!HH", ACK_PACKET, sequence_number)
                sock.send(ack)
                logger.info("Sent ACK for sequence number %d", sequence_number)
            else:
                # Handle packet loss or corruption
                logger.info("Received out-of-sequence packet or corrupted packet. Ignoring.")
        except socket.timeout:
            logger.info("Timed out durring reception")
        except socket.error as e:
            logger.error("Socket error occurred: %s", str(e))
            break

    return num_bytes
