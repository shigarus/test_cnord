import logging
from typing import Callable

from tornado.iostream import StreamClosedError, IOStream
from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer

import config
import message
import store


class Application:

    def __init__(self):
        self._sources_server = SourcesServer(
            on_connect=self._on_source_connect,
            on_msg=self._on_source_msg,
            on_close=self._on_source_close,
        )
        self._listeners_server = ListenersServer(
            on_connect=self._on_listener_connect,
            on_close=self._on_listener_close,
        )
        self._sources_connects = {}

    def listen(self, sources_port, listeners_port):
        self._sources_server.listen(sources_port)
        self._listeners_server.listen(listeners_port)

    def _on_source_connect(self, source_stream: IOStream):
        pass

    async def _on_source_msg(self, source_stream: IOStream, msg: bytes):
        parsed = message.parse_source_bytes(msg)
        if not parsed:
            answer_to_source = message.gen_answer_to_source(success=False)
            await source_stream.write(answer_to_source)
            return
        source_id = parsed['source_id']
        store.SourcesStore.update_state(
            source_id=source_id,
            serial_num=parsed['num'],
            state=parsed['source_state'],
        )
        if source_id not in self._sources_connects:
            self._sources_connects[source_id] = source_stream
        answer_to_source = message.gen_answer_to_source(
            success=True,
            serial_num=parsed['num'],
        )
        await source_stream.write(answer_to_source)
        self._send_to_listeners(parsed['msgs'])

    async def _send_to_listeners(self, msgs):
        pass

    def _on_source_close(self, source_stream: IOStream):
        pass

    def _on_listener_connect(self, listener_stream: IOStream):
        pass

    def _on_listener_close(self, listener_stream: IOStream):
        pass


class SourcesServer(TCPServer):
    """
    Manages connects to sources.
    """

    def __init__(self,
                 on_connect: Callable[[IOStream]],
                 on_msg: Callable[[IOStream, bytes]],
                 on_close: Callable[[IOStream]],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_connect = on_connect
        self._on_msg = on_msg
        self._on_close = on_close

    async def handle_stream(self, stream: IOStream, address: str):
        self._on_connect(IOStream)
        try:
            while True:
                msg: bytes = await stream.read_until_close()
                await self._on_msg(stream, msg)
        except StreamClosedError:
            self._on_close(stream)


class ListenersServer(TCPServer):
    """
    Manages connects to listeners.
    """

    def __init__(self,
                 on_connect: Callable[[IOStream]],
                 on_close: Callable[[IOStream]],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_connect = on_connect
        self._on_close = on_close

    def handle_stream(self, stream: IOStream, address: str):
        self._on_connect(stream)
        stream.set_close_callback(lambda: self._on_close(stream))


def main():
    conf = config.get_config()
    logging.basicConfig(
        level=logging.DEBUG if conf['debug'] else logging.INFO,
        format='%(levelname)s:%(asctime)s:%(message)s',
    )
    app = Application()
    app.listen(
        sources_port=conf['sources_port'],
        listeners_port=conf['listeners_port'],
    )
    IOLoop.current().start()


if __name__ == '__main__':
    main()
