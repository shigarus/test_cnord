"""
Module is used for working with net messaging protocols
"""
import functools


BYTE_ORDER = 'big'

state_translate = {
    0x01: 'IDLE',
    0x02: 'ACTIVE',
    0x03: 'RECHARGE',
}


def gen_answer_to_source(success: bool, serial_num: int = None) -> bytes:
    """
    Generates bytes msg as:
    1 byte - header. 0x11 if success else 0x12
    2 bytes - serial num or 0x00 0x00 if not succeeded
    1 byte - XOR of the message
    """
    if success:
        b_serial_num = serial_num.to_bytes(2, byteorder=BYTE_ORDER)
    else:
        b_serial_num = bytes([0x00, 0x00])
    res = bytes([
        0x11 if success else 0x12,
        *b_serial_num,
    ])
    return bytes([*res, *xor(res)])


def parse_source_bytes(bytes_obj: bytes) -> dict:
    """
    Transforms bytes received from source to dict
    :return: dict(
        header=0x01,
        num=int,  # serial number of the message
        source_id=str,  # identifier of the source
        source_state=byte,  # acceptable values: 0x01 --IDLE, 0x02 --ACTIVE, 0x03 --RECHARGE
        msgs=Sequence[Tuple[str, int]], # every item is pair of (name, value) or None if message is corrupted
    )
    """
    if len(bytes_obj) < 13 or bytes_obj[0] != 0x01:
        return {}
    num = int.from_bytes(bytes_obj[1:3], byteorder=BYTE_ORDER, signed=False)
    source_id = str(bytes_obj[3:11], encoding='ascii')
    source_state = state_translate.get(bytes_obj[11])
    if source_state is None:
        return {}
    num_of_msgs = int.from_bytes(bytes([bytes_obj[12]]), byteorder=BYTE_ORDER, signed=False)
    byte_msgs = bytes_obj[13:]
    if len(byte_msgs) != num_of_msgs*13:
        return {}
    correct_msgs = filter(None, _iter_source_msgs(byte_msgs))
    return dict(
        header=0x01,
        num=num,
        source_id=source_id,
        source_state=source_state,
        msgs=[msg for msg in correct_msgs],
    )


def _iter_source_msgs(byte_msgs: bytes):
    cur_pos = 0
    while cur_pos+8 < len(byte_msgs):
        name = byte_msgs[cur_pos:cur_pos+8]
        value = int.from_bytes(byte_msgs[cur_pos+8:cur_pos+12], byteorder=BYTE_ORDER)
        xor_byte = xor(byte_msgs[cur_pos:cur_pos+12])
        if xor_byte != bytes([byte_msgs[cur_pos+12]]):
            yield None
        else:
            yield (name, value)
        cur_pos += 13


def xor(bytes_obj: bytes) -> bytes:
    """ Returns XOR of bytes_obj """
    res = functools.reduce(
        lambda res, b: res ^ b,
        bytes_obj[1:],
        bytes_obj[0],
    )
    return bytes([res])
