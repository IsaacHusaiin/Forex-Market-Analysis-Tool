import struct
import socket
from datetime import datetime, timezone


# Serialize an IP address and port for subscription
def serialize_address(address: tuple) -> bytes:
    """
    Convert an IP address and port number into a serialized byte format.

    Args:
        address (tuple): A tuple containing the IP address (str) and port (int).

    Returns:
        bytes: Serialized byte data representing the IP and port.
    """
    ip_address, port = address
    ip_packed = socket.inet_aton(ip_address)  # Convert IP to packed 4-byte format
    port_packed = struct.pack('>H', port)  # Convert port to 2-byte big-endian format
    return ip_packed + port_packed


# Deserialize the subscription address
def deserialize_address(data: bytes) -> (str, int):
    """
    Deserialize a serialized IP address and port from byte format.

    Args:
        data (bytes): Serialized byte data for the IP address and port.

    Returns:
        tuple: A tuple containing the IP address (str) and port (int).
    """
    ip_packed = data[:4]  # Extract the first 4 bytes for the IP address
    port_packed = data[4:6]  # Extract the next 2 bytes for the port

    ip_address = socket.inet_ntoa(ip_packed)  # Convert packed IP back to string format
    port = struct.unpack('>H', port_packed)[0]  # Unpack port from 2-byte big-endian format
    return ip_address, port


# Marshal quotes for sending
def marshal_message(quote_sequence):
    """
    Convert a list of quotes into a single binary message.

    Args:
        quote_sequence (list): A list of dictionaries, where each dictionary
                               contains information for a quote ('cross', 'price', 'time').

    Returns:
        bytes: A binary message containing serialized quote information.
    """
    message = bytes()  # Initialize an empty byte string to build the message

    for quote in quote_sequence:
        # Encode currency pair as ASCII strings
        currency1 = quote['cross'][:3].encode('ascii')  # First currency code (e.g., 'USD')
        currency2 = quote['cross'][4:].encode('ascii')  # Second currency code (e.g., 'JPY')

        # Pack the exchange rate as a 4-byte little-endian float
        rate = struct.pack('<f', quote['price'])

        # Convert timestamp to microseconds and pack it as an 8-byte big-endian integer
        timestamp = quote.get('time')
        if not isinstance(timestamp, (int, float)):
            timestamp = int(datetime.now(timezone.utc).timestamp() * 1_000_000)  # Current timestamp if missing
        else:
            timestamp = int(timestamp * 1_000_000)  # Convert to microseconds
        timestamp = struct.pack('>Q', timestamp)

        # Add padding to complete the 32-byte structure
        padding = b'\x00' * 14  # 14 bytes of zero padding for alignment

        # Append serialized quote data to the message
        message += currency1 + currency2 + rate + timestamp + padding

    return message


# Unmarshal (decode) incoming quotes
def unmarshal_message(data: bytes) -> list:
    """
    Extract and decode a list of quotes from a binary message.

    Args:
        data (bytes): The binary message containing multiple 32-byte serialized quotes.

    Returns:
        list: A list of dictionaries, each containing 'currency1', 'currency2', 'rate', and 'timestamp'.
    """
    quote_size = 32  # Each quote occupies a fixed size of 32 bytes in the message
    quotes = []  # Initialize an empty list to store parsed quotes

    for i in range(0, len(data), quote_size):
        chunk = data[i:i + quote_size]  # Extract a 32-byte chunk for each quote

        # Decode currency codes from the first 6 bytes
        currency1 = chunk[0:3].decode('ascii')  # Decode the first currency (3 bytes)
        currency2 = chunk[3:6].decode('ascii')  # Decode the second currency (3 bytes)

        # Unpack the rate (float, little-endian) and timestamp (big-endian integer)
        rate = struct.unpack('<f', chunk[6:10])[0]  # Extract the exchange rate as float
        timestamp = struct.unpack('>Q', chunk[10:18])[0]  # Extract timestamp in microseconds

        # Store the parsed quote details in a dictionary
        quote = {
            'currency1': currency1,
            'currency2': currency2,
            'rate': rate,
            'timestamp': timestamp
        }
        quotes.append(quote)  # Append the parsed quote to the list

    return quotes