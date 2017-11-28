import unittest
from itertools import chain

import message


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
            res = message.parse_source_bytes(inp)
            assert res == out, f'received {res} expects  {out}'

    def setUp(self):
        self.messages = (
            (b'uierwuie', 2344),
            (b'uierwuis', 2346),
            (b'uiersuis', 2244),
            (b'ugerwuis', 5344),
        )
        self.messages_as_bytes = [
            bytes([
                *key,
                *val.to_bytes(4, byteorder='big'),
                *message.xor(bytes([*key, *val.to_bytes(2, byteorder='big')]))
            ])
            for key, val in self.messages
        ]
        self.correct_meta = 0x01, 0x00, 0x00, *b'asdfghjk', 0x03
        self.correct_answer_to_meta = dict(
            header=0x01,
            num=0,
            source_id='asdfghjk',
            source_state=0x003,
        )

    def test_empty_msgs(self):
        inp = bytes([*self.correct_meta, 0x00])
        out = dict(msgs=[])
        out.update(self.correct_answer_to_meta)
        res = message.parse_source_bytes(inp)
        assert res == out, f'received {res} expects  {out}'

    def test_one_msg(self):
        inp = bytes([*self.correct_meta, 0x01, *self.messages_as_bytes[0]])
        out = dict(msgs=[self.messages[0]])
        out.update(self.correct_answer_to_meta)
        res = message.parse_source_bytes(inp)
        assert res == out, f'received {res} expects  {out}'

    def test_several_msgs(self):
        inp = bytes([*self.correct_meta, len(self.messages), *chain.from_iterable(self.messages_as_bytes)])
        out = dict(msgs=list(self.messages))
        out.update(self.correct_answer_to_meta)
        res = message.parse_source_bytes(inp)
        assert res == out, f'received {res} expects  {out}'

    def test_with_invalid_message(self):
        inp = bytes([*self.correct_meta, len(self.messages), *chain.from_iterable(self.messages_as_bytes)])
        changed_byte = inp[-1] + 1
        inp = bytes([*inp[:-1], changed_byte])
        out = dict(msgs=[
            msg if i != len(self.messages) - 1 else None
            for i, msg in enumerate(self.messages)
        ])
        out.update(self.correct_answer_to_meta)
        res = message.parse_source_bytes(inp)
        assert res == out, f'received {res} expects  {out}'
