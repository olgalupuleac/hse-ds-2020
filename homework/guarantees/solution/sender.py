#!/usr/bin/env python

import argparse
import logging
import threading, queue
import copy
from dslib import Communicator, Message


class Sender:
    def __init__(self, name, recv_addr):
        self._comm = Communicator(name)
        self._recv_addr = recv_addr
        self._command_queue = []
        self._ok_responses = set()

    def _send_at_least_once(self, msg, *, handle_local_message):
        while True:
            self._comm.send(msg, self._recv_addr)
            required_response = copy.deepcopy(msg)
            required_response._sender = self._recv_addr
            response = self._comm.recv(1)
            if response is not None:
                if response.is_local():
                    handle_local_message(response)
                else:
                    self._ok_responses.add(response)
            if required_response in self._ok_responses:
                return

    def _process_message(self, msg):

        # deliver INFO-1 message to receiver user
        # underlying transport: unreliable with possible repetitions
        # goal: receiver knows all that were recieved but at most once
        if msg.type == 'INFO-1':
            self._comm.send(msg, self._recv_addr)

        # deliver INFO-2 message to receiver user
        # underlying transport: unreliable with possible repetitions
        # goal: receiver knows all at least once
        elif msg.type == 'INFO-2':
            self._send_at_least_once(msg, handle_local_message=self._process_message)


        # deliver INFO-3 message to receiver user
        # underlying transport: unreliable with possible repetitions
        # goal: receiver knows all exactly once
        elif msg.type == 'INFO-3':
            self._send_at_least_once(msg, handle_local_message=self._process_message)

        # deliver INFO-4 message to receiver user
        # underlying transport: unreliable with possible repetitions
        # goal: receiver knows all exactly once in the order
        elif msg.type == 'INFO-4':
            self._send_at_least_once(msg, handle_local_message=lambda m: self._command_queue.append(m))

        else:
            err = Message('ERROR', 'unknown command: %s' % msg.type)
            self._comm.send_local(err)

    def run(self):
        while True:
            if self._command_queue:
                msg = self._command_queue[0]
                self._command_queue.pop(0)
            else:
                msg = self._comm.recv_local()
            self._process_message(msg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', dest='recv_addr', metavar='host:port',
                        help='receiver address', default='127.0.0.1:9701')
    parser.add_argument('-d', dest='log_level', action='store_const', const=logging.DEBUG,
                        help='print debugging info', default=logging.WARNING)
    args = parser.parse_args()
    logging.basicConfig(format="%(asctime)s - %(message)s", level=args.log_level)

    sender = Sender('sender', args.recv_addr)
    sender.run()


if __name__ == "__main__":
    main()
