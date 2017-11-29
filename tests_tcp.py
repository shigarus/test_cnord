import socket

from message import BYTE_ORDER
from tests import with_xor


class SourceTCP:
    """ Implements model of Source """

    def __init__(self, name: bytes):
        assert len(name) == 8
        self._sock = socket.socket()
        self._sock.connect(('localhost', 8888))
        self.name = name
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


def test():
    source_client = SourceTCP(b'basderty')
    correct = b'\x11\x00\x01\x10'
    incorrect = b'\x12\x00\x00\x12'
    assert source_client.send_correct_empty() == correct
    assert source_client.send_incorrect_inner() == incorrect
    msgs = (b'asdfqwer', 1), (b'yuiohjkl', 2)
    assert source_client.send_correct_msgs(*msgs) == correct
    listener_client = ListenerTCP()
    print(listener_client.recv_msg())
    source_client.send_correct_msgs(*msgs)
    print(listener_client.recv_msg())


if __name__ == '__main__':
    test()
