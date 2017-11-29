"""
Tests system as a whole.
app.py has to be ran before this.
"""
import socket

from source_protocol import BYTE_ORDER
from .test_source_protocol import with_xor


class SourceTCP:
    """ Implements model of Source """

    def __init__(self, name: str):
        assert len(name) == 8
        self._sock = socket.socket()
        self._sock.connect(('localhost', 8888))
        self.name = bytes(name, encoding='ascii')
        self._meta = 0x01, 0x00, 0x01, *self.name, 0x01

    def _request(self, msgs_len, msgs_bytes):
        self._sock.send(bytes([*self._meta, msgs_len, *msgs_bytes]))
        return self._sock.recv(1024)

    def send_correct_empty(self):
        return self._request(0, b'')

    def send_incorrect_inner(self):
        byte_msg = bytes([
            *b'asdsqwer',
            *(3).to_bytes(4, byteorder=BYTE_ORDER),
        ])
        xor = with_xor(byte_msg)[-1]
        corrupted = bytes([*byte_msg, xor+1])
        return self._request(1, corrupted)

    def send_correct_msgs(self, *msgs):
        msgs_as_bytes = (
            bytes([*k, *v.to_bytes(4, byteorder=BYTE_ORDER)])
            for k, v in msgs
        )
        msgs_as_bytes_with_xor = (
            with_xor(msg)
            for msg in msgs_as_bytes
        )
        return self._request(len(msgs), b''.join(msgs_as_bytes_with_xor))


class ListenerTCP:
    """ Implements model of Listener """

    def __init__(self):
        self._sock = socket.socket()
        self._sock.connect(('localhost', 8889))
        self._registered_sources = {}

    def recv_msg(self):
        return str(self._sock.recv(512), encoding='ascii')


def _source_to_listener_msgs(source_name, msgs):
    return ''.join(
        f'[{source_name}] {str(key, encoding="ascii")} | {val}\r\n'
        for key, val in msgs
    )


def test():
    source_name1 = 'basderty'
    source_client1 = SourceTCP(source_name1)
    correct = b'\x11\x00\x01\x10'
    incorrect = b'\x12\x00\x00\x12'
    # check sending and receiving messages only as source
    assert source_client1.send_correct_empty() == correct
    assert source_client1.send_incorrect_inner() == incorrect
    msgs = (b'asdfqwer', 1), (b'yuiohjkl', 2)
    assert source_client1.send_correct_msgs(*msgs) == correct

    # add listener. It has to receive info about existing source_client
    listener_client1 = ListenerTCP()
    received = listener_client1.recv_msg()
    source_state = f'[{source_name1}] 1 | IDLE '
    assert received.startswith(source_state)

    # check if listener receives messages, which sent by source_client
    source_client1.send_correct_msgs(*msgs)
    received = listener_client1.recv_msg()
    expected = _source_to_listener_msgs(source_name1, msgs)
    assert received == expected

    # check adding another source
    source_name2 = 'asdftrew'
    source_client2 = SourceTCP(source_name2)
    source_client2.send_correct_msgs(*msgs)
    # listener has to receive info about new source
    received = listener_client1.recv_msg()
    source_state2 = f'[{source_name2}] 1 | IDLE | '
    assert received.startswith(source_state2)
    # and only after - messages
    received = listener_client1.recv_msg()
    expected = _source_to_listener_msgs(source_name2, msgs)
    assert received == expected

    # check if new listener will receive info about several sources
    listener_client2 = ListenerTCP()
    received = listener_client2.recv_msg().strip()
    source1_received, source2_received = received.split('\r\n')
    assert source1_received.startswith(source_state) and source2_received.startswith(source_state2)

    # check if both listeners will receive new msg
    msgs = (b'asdgerty', 20), (b'uiopvbnm', 33)
    source_client1.send_correct_msgs(*msgs)
    expected = _source_to_listener_msgs(source_name1, msgs)
    listener1_received = listener_client1.recv_msg()
    listener2_received = listener_client2.recv_msg()
    assert listener1_received == listener2_received == expected


if __name__ == '__main__':
    test()
