import unittest
from itertools import chain

import source_protocol


def with_xor(bytes_obj: bytes) -> bytes:
    return bytes([*bytes_obj, *source_protocol.xor(bytes_obj)])


class TestParseSourceBytes(unittest.TestCase):

    def test_incorrect_cases(self):
        incorrect_cases = {
            bytes(): {},
            bytes([0x00]): {},
            bytes([0x01, 0x00, 0x00, *b'asdfghjk', 0x04, 0x00]): {},
            # has too long msgs part
            bytes([0x01, 0x00, 0x00, *b'asdfghjk', 0x03, 0x01, *b'uierwuie', 0x32, 0x43, 0x54, 0x34, 0x32, 0x34]): {},
        }
        for inp, out in incorrect_cases.items():
            res = source_protocol.parse_source_bytes(inp)
            assert res == out, f'received {res} expects  {out}'

    def setUp(self):
        self.messages = (
            (b'uierwuie', 2344),
            (b'uierwuis', 2346),
            (b'uiersuis', 2244),
            (b'ugerwuis', 5344),
        )
        self.messages_as_bytes = (
            bytes([
                *key,
                *val.to_bytes(4, byteorder='big'),
            ])
            for key, val in self.messages
        )
        self.messages_as_bytes = [
            with_xor(msg)
            for msg in self.messages_as_bytes
        ]
        self.correct_meta = 0x01, 0x00, 0x00, *b'asdfghjk', 0x03
        self.correct_answer_to_meta = dict(
            header=0x01,
            num=0,
            source_id='asdfghjk',
            source_state='RECHARGE',
        )

    def test_empty_msgs(self):
        inp = bytes([*self.correct_meta, 0x00])
        out = dict(msgs=[])
        out.update(self.correct_answer_to_meta)
        res = source_protocol.parse_source_bytes(inp)
        assert res == out, f'received {res} expects  {out}'

    def test_one_msg(self):
        inp = bytes([*self.correct_meta, 0x01, *self.messages_as_bytes[0]])
        out = dict(msgs=[self.messages[0]])
        out.update(self.correct_answer_to_meta)
        res = source_protocol.parse_source_bytes(inp)
        assert res == out, f'received {res} expects  {out}'

    def test_several_msgs(self):
        inp = bytes([*self.correct_meta, len(self.messages), *chain.from_iterable(self.messages_as_bytes)])
        out = dict(msgs=list(self.messages))
        out.update(self.correct_answer_to_meta)
        res = source_protocol.parse_source_bytes(inp)
        assert res == out, f'received {res} expects  {out}'

    def test_with_invalid_message(self):
        inp = bytes([*self.correct_meta, len(self.messages), *chain.from_iterable(self.messages_as_bytes)])
        changed_byte = inp[-1] + 1
        inp = bytes([*inp[:-1], changed_byte])
        res = source_protocol.parse_source_bytes(inp)
        assert res == {}, f'received {res} expects empty dict'


class TestGenAnswerToSource(unittest.TestCase):

    def test_fail(self):
        res = source_protocol.gen_answer_to_source(False)
        assert res == with_xor(bytes([0x12, 0x00, 0x00])), f'got {res}'

    def test_succes(self):
        res = source_protocol.gen_answer_to_source(True, 2)
        assert res == with_xor(bytes([0x11, 0x00, 0x02])), f'got {res}'
