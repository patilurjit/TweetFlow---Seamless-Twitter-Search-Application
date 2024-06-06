# -*- coding: utf-8 -*-
"""

@author: Atharva & Urjit
"""


import pickle
import time


class Cache:

    """
    A cache class that stores key-value pairs in a dictionary and evicts the oldest key if the maximum size is exceeded.

    Attributes:
    max_size (int): The maximum size of the cache.
    cache (dict): The dictionary to store the cache key-value pairs.
    key_times (list): The list to keep track of the time when keys were added.
    checkpoint_file (str): The file to save cache state to.
    checkpoint_interval (int): The interval for saving cache state.
    """

    def __init__(self, checkpoint_file=None, checkpoint_interval=None):
        """
        This function initializes the Cache object.

        Parameters:
        checkpoint_file (str): The file to save cache state to.
        checkpoint_interval (int): The interval for saving cache state.
        """
        
        self.max_size = 10  # Maximum size of the cache
        self.cache = {}  # Dictionary to store cache key-value pairs
        self.key_times = []  # List to keep track of the time when keys were added
        self.checkpoint_file = checkpoint_file  # File to save cache state to
        self.checkpoint_interval = (
            checkpoint_interval  # Interval for saving cache state
        )
        if checkpoint_file:
            self.load_checkpoint()  # Load cache state from file if checkpoint_file is provided

    def get(self, key):
        """
        This function retrieves the value associated with the key.

        Parameters:
        key (str): The key to retrieve the value for.

        Returns:
        The value associated with the key, or None if the key is not in the cache.
        """

        if key in self.cache:
            return self.cache[
                key
            ]  # Return the value associated with the key if it's in the cache
        else:
            return None  # Otherwise, return None

    def set(self, key, value):
        """
        This function adds the key-value pair to the cache, evicting the oldest key if the maximum size is exceeded.

        Parameters:
        key (str): The key to add to the cache.
        value: The value associated with the key.
        
        Returns:
        None
        """

        if len(self.cache) >= self.max_size:
            # Remove the oldest key
            oldest_key = min(self.key_times, key=lambda x: x[1])[
                0
            ]  # Find the oldest key using key_times
            self.key_times = [
                x for x in self.key_times if x[0] != oldest_key
            ]  # Remove the oldest key from key_times
            self.cache.pop(oldest_key)  # Remove the oldest key-value pair from cache

        self.cache[key] = value  # Add the key-value pair to the cache
        self.key_times.append(
            (key, time.time())
        )  # Add the current time to key_times for the new key

        if (
            self.checkpoint_file
            and self.checkpoint_interval
            and len(self.cache) % self.checkpoint_interval == 0
        ):
            self.save_checkpoint()  # Save the cache state if checkpoint_file and checkpoint_interval are provided

    def save_checkpoint(self):
        """
        This function saves the cache state to the checkpoint file.
        """

        with open(self.checkpoint_file, "wb") as file:
            pickle.dump(
                self.cache, file
            )  # Serialize the cache dictionary and save it to the checkpoint file

    def load_checkpoint(self):
        """
        This function loads the cache state from the checkpoint file.
        """

        try:
            with open(self.checkpoint_file, "rb") as file:
                self.cache = pickle.load(
                    file
                )  # Deserialize the cache dictionary from the checkpoint file
                self.key_times = [
                    (k, time.time()) for k in self.cache.keys()
                ]  # Reset the key_times list
        except FileNotFoundError:
            pass  # Ignore the exception if the checkpoint file is not found