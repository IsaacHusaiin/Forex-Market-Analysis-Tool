"""
Author: Isaac Husaiin
Filename: lab3.py
Date: Oct 30 2024
Purpose: Implement a Forex subscriber that connects to a Forex provider, subscribes for currency quotes,
         and identifies arbitrage opportunities based on negative cycle detection in currency exchange rates.
         The subscriber processes incoming Forex quotes, constructs a weighted graph representing the exchange
         rates, and uses the Bellman-Ford algorithm to detect arbitrage cycles that would yield a profit.
"""

import socket
import math
from datetime import datetime, timedelta
from bellman_ford import BellmanFord
import fxp_bytes_subscriber

# Define QUOTE_EXPIRY outside the class as a constant
QUOTE_EXPIRY = timedelta(seconds=1.5)

class ForexSubscriber:
    """
    A class that represents a Forex subscriber connecting to a Forex provider to receive and process
    currency exchange quotes, check for stale quotes, and detect arbitrage opportunities using the Bellman-Ford algorithm.

    Attributes:
        forex_provider_address (tuple): Address of the Forex provider (host, port).
        listening_ip (str): IP address for listening to quotes.
        listening_port (int): Port for listening to quotes.
        BUFFER_SIZE (int): Maximum buffer size for receiving messages.
        latest_timestamp (dict): Dictionary to track the latest timestamp for each currency pair.
        graph (dict): A dictionary structure to store the currency exchange rates as a graph.
        quotes_dict (dict): Dictionary to store the current quotes with their respective timestamps.
        start_time (datetime): Time at which the subscription started.
        total_session_profit (float): Tracks total profit accumulated during the session.
    """
    def __init__(self):
        """
        Initializes the ForexSubscriber with provider details, buffer size,
        and dictionaries to manage received data and detect arbitrage opportunities.
        """
        self.forex_provider_address = ('localhost', 10203)
        self.listening_ip = '127.0.0.1'
        self.listening_port = 10000
        self.BUFFER_SIZE = 4096
        self.latest_timestamp = {}  # Track the latest timestamp for each market
        self.graph = {}  # To store the exchange rates as a graph
        self.quotes_dict = {}  # Store current quotes with timestamps
        self.start_time = datetime.utcnow()
        self.total_session_profit = 0.0  # Track total profit for the session



    def send_subscription_request(self):
        """
        Sends a subscription request to the Forex provider with the local address for receiving quotes.
        """
        # Define local address for receiving quotes
        local_address = (self.listening_ip, self.listening_port)

        # Serialize address and send subscription request via UDP socket
        data = fxp_bytes_subscriber.serialize_address(local_address)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as subscription_socket:
            subscription_socket.sendto(data, self.forex_provider_address)

        # Print confirmation message
        print(f"Subscribed to Forex Publisher with local address {local_address}")


    def receive_forex_quotes(self):
        """
        Receives Forex quotes from the provider, processes them, checks for stale quotes,
        and detects arbitrage opportunities within a defined subscription phase.

        This method listens for quotes on a UDP socket and handles timeouts to shut down
        if no messages are received for a prolonged period.
        """
        subscription_phase = timedelta(minutes=10)
        self.start_time = datetime.utcnow()
        timeout_start = None  # To track consecutive timeouts

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiving_socket:
            receiving_socket.bind((self.listening_ip, self.listening_port))
            receiving_socket.settimeout(10)  # 10-second timeout for receiving messages
            print(f"Listening for Forex Quotes on {self.listening_ip}:{self.listening_port}")

            while datetime.utcnow() - self.start_time < subscription_phase:
                try:
                    # Receive messages with a buffer size of 4096 bytes
                    data, addr = receiving_socket.recvfrom(self.BUFFER_SIZE)
                    last_message_time = datetime.utcnow()  # Update the last received message time

                    # Reset timeout_start since a message was received
                    timeout_start = None

                    # Process and unmarshal the incoming message
                    quotes = fxp_bytes_subscriber.unmarshal_message(data)
                    self.process_quotes(quotes)
                    self.remove_stale_quotes()
                    self.detect_arbitrage()

                except socket.timeout:
                    print("No messages received for 10 seconds \nShutting down in 10 more seconds")

                    # Start the timeout tracking if it's the first timeout
                    if timeout_start is None:
                        timeout_start = datetime.utcnow()
                    # Check if 10 additional seconds have passed since the first timeout
                    elif datetime.utcnow() - timeout_start >= timedelta(seconds=10):
                        print("\nShutting down!")
                        break  # Exit the loop and shut down
            print(f"\nTotal session profit: {self.total_session_profit:.2f} USD")
            print(f"\n\nBye-Bye")


    def process_quotes(self, quotes):
        """
        Processes the received Forex quotes by updating timestamps and prices in the dictionary,
        and updates the graph with the latest rates.
        """
        for quote in quotes:
            # Use get with default values or handle missing keys gracefully
            currency1 = quote.get('currency1')
            currency2 = quote.get('currency2')
            rate = quote.get('rate')
            timestamp = quote.get('timestamp')

            # Validate fields to avoid processing incomplete data
            if not all([currency1, currency2, rate, timestamp]):
                print(f"Skipping incomplete quote: {quote}")
                continue

            # Proceed if all required fields are available
            market = f"{currency1}/{currency2}"
            formatted_time = datetime.utcfromtimestamp(timestamp / 1_000_000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"{formatted_time} {currency1} {currency2} {rate}")

            # Handle out-of-sequence quotes
            if market in self.latest_timestamp and timestamp <= self.latest_timestamp[market]:
                print(f"Ignoring out-of-sequence message for {market} at {formatted_time}")
                continue

            # Update latest timestamp and quote dictionary
            self.latest_timestamp[market] = timestamp
            self.quotes_dict[market] = {'price': rate, 'time': datetime.utcnow()}

            # Update graph
            weight = -math.log(rate)
            self.graph.setdefault(currency1, {})[currency2] = weight
            self.graph.setdefault(currency2, {})[currency1] = -math.log(1 / rate)

    def remove_stale_quotes(self):
        """
        Removes quotes from `quotes_dict` that have become stale based on the defined QUOTE_EXPIRY period.
        """
        # Get the current time to compare timestamps of quotes
        current_time = datetime.utcnow()

        # Iterate over a copy of `quotes_dict` keys to safely remove items during iteration
        for market in list(self.quotes_dict.keys()):
            # Retrieve the timestamp of each quote
            quote_time = self.quotes_dict[market]['time']

            # Check if the quote is stale by comparing its age to QUOTE_EXPIRY
            if current_time - quote_time > QUOTE_EXPIRY:
                print(f"Removing stale quote for {market}")
                del self.quotes_dict[market]  # Remove stale quote

    def build_graph(self):
        """
        Constructs a weighted graph using current quotes where edges represent currency exchange rates,
        with weights as the negative logarithms of the rates.

        Returns:
            BellmanFord: An instance of the BellmanFord class with the constructed graph.
        """
        bf = BellmanFord()  # Initialize an instance of BellmanFord to build the graph

        # Iterate through each currency pair and rate in quotes_dict to add to the graph
        for market, quote in self.quotes_dict.items():
            rate = quote['price']  # Get the exchange rate for the currency pair
            currency1, currency2 = market.split('/')  # Split the currency pair

            # Calculate the weight as the negative logarithm of the rate
            weight = -math.log(rate)
            bf.add_edge(currency1, currency2, weight)  # Add directed edge to the graph

        return bf  # Return the constructed graph with currency exchange rates

    def find_arbitrage(self, bf):
        """
        Uses the Bellman-Ford algorithm to find negative cycles (arbitrage opportunities)
        starting from the currency 'USD'.

        Args:
            bf (BellmanFord): An instance of the BellmanFord class containing the currency graph.

        Returns:
            tuple: Contains the edge of the negative cycle (if found) and the predecessor dictionary.
        """
        start_currency = 'USD'  # Set 'USD' as the starting currency for finding arbitrage
        if start_currency not in bf.vertices:
            return None, None  # Return if 'USD' is not in the graph vertices

        # Execute the Bellman-Ford algorithm to find shortest paths from 'USD'
        distance, predecessor, negative_cycle_edge = bf.shortest_paths(start_currency)

        # Return the edge of the negative cycle and predecessor if a cycle is found, otherwise None
        return negative_cycle_edge, predecessor if negative_cycle_edge else (None, None)

    def detect_arbitrage(self):
        """
        Detects arbitrage opportunities by building the graph and finding any negative cycles.
        If a negative cycle is detected, it is passed to the display method.
        """
        bf = self.build_graph()  # Build the graph with current exchange rates
        negative_cycle_edge, predecessor = self.find_arbitrage(bf)  # Look for negative cycles

        # Check if a negative cycle was found
        if negative_cycle_edge:
            cycle = self.reconstruct_negative_cycle(negative_cycle_edge, predecessor)  # Reconstruct the cycle path

            # Only display arbitrage opportunities that involve 'USD'
            if 'USD' in cycle:
                self.display_arbitrage(cycle)
            else:
                print("Negative cycle does not include USD. Skipping.")
        else:
            print("\nNo arbitrage opportunity detected at this time.")  # Indicate no arbitrage detected


    def reconstruct_negative_cycle(self, negative_cycle_edge, predecessor):
        """
        Reconstructs the cycle of currencies that constitute an arbitrage opportunity.

        Args:
            negative_cycle_edge (tuple): The edge representing the negative cycle.
            predecessor (dict): A dictionary of predecessors for each vertex.

        Returns:
            list: Ordered list of currencies in the detected arbitrage cycle.
        """
        u, v = negative_cycle_edge  # Start from the negative cycle edge
        cycle = [v]
        current = u

        # Track visited nodes to prevent looping in case of errors in cycle tracking
        visited = set()
        while current != v and current is not None:
            if current in visited:
                break  # Break if a loop is detected in the cycle path
            visited.add(current)
            cycle.append(current)
            current = predecessor.get(current)  # Traverse back using the predecessor links

        cycle.append(v)  # Complete the cycle by adding the start node
        cycle.reverse()  # Reverse to get the correct order from start to end

        # Ensure the cycle is valid before returning
        if len(cycle) < 3:
            print("Detected incomplete or incorrect cycle, skipping.")
            return []

        return cycle

    def display_arbitrage(self, cycle):
        """
        Displays details of the arbitrage opportunity, including each exchange in the cycle,
        and calculates the total profit in the starting currency.

        Args:
            cycle (list): Ordered list of currencies involved in the arbitrage cycle.
        """
        if 'USD' not in cycle:
            print("No USD cycle. Skipping.")  # Skip if USD is not part of the cycle
            return
        while cycle[0] != 'USD':  # Rotate the cycle to start from USD
            cycle.append(cycle.pop(0))

        print("ARBITRAGE:")
        starting_amount = 100.0
        amount = starting_amount
        print(f"\tStart with {cycle[0]} {amount}")  # Starting amount in USD
        for i in range(len(cycle) - 1):
            curr_from, curr_to = cycle[i], cycle[i + 1]
            cross = f"{curr_from}/{curr_to}"
            # Retrieve the exchange rate, or use the inverse rate if unavailable
            rate = self.quotes_dict.get(cross, {}).get('price') or 1 / self.quotes_dict[f"{curr_to}/{curr_from}"][
                'price']
            amount *= rate  # Update amount after conversion
            print(f"\tExchange {curr_from} for {curr_to} at {rate:.6f} --> {curr_to} {amount:.6f}")

        profit = amount - starting_amount  # Calculate profit
        self.total_session_profit += profit  # Accumulate profit for the session
        print(f"\n\tTotal profit: {profit:.2f} {cycle[0]}")  # Display total profit
        print()

    def run(self):
        """
        Runs the Forex subscriber by sending a subscription request and initiating the quote receiving process.
        """
        self.send_subscription_request()
        self.receive_forex_quotes()


if __name__ == '__main__':
    subscriber = ForexSubscriber()
    subscriber.run()