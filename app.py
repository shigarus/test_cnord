import datetime
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
        self._listeners_connects = {}

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
        self._send_to_listeners(parsed['msgs'], source_id)

    def _on_source_close(self, source_stream: IOStream):
        source_id = next(
            (k for k, v in self._sources_connects if v is source_stream),
            None,
        )
        if source_id:
            self._sources_connects.pop(source_id)

    def _on_listener_connect(self, listener_stream: IOStream):
        id_ = store.ListenerStore.add_listener()
        self._listeners_connects[id_] = listener_stream
        self._notify_about_sources(listener_stream, id_)

    def _on_listener_close(self, listener_stream: IOStream):
        listener_id = next(
            (k for k, v in self._listeners_connects if v is listener_stream),
            None,
        )
        if listener_id:
            self._listeners_connects.pop(listener_id)

    @staticmethod
    async def _notify_about_sources(listener_stream, id_):
        sources = store.SourcesStore.get_all()
        str_per_source = (
            _gen_notify_aboute_source_msg(source)
            for source in sources
        )
        await listener_stream.write(''.join(str_per_source))
        for source in sources:
            store.ListenerStore.set_notified(source.id_)

    async def _send_to_listeners(self, msgs, source_id):
        for listener in store.ListenerStore.get_all():
            listener_stream = self._listeners_connects[listener.id_]

            # ensure listener knows about source before sending him current messages
            if source_id not in listener.sources_notified:
                source = store.SourcesStore.get_state(source_id)
                await listener_stream.write(_gen_notify_aboute_source_msg(source))

            msgs_without_none = filter(None, msgs)
            listener_msg = ''.join(
                f"[{source_id}] {key} | {value}\r\n"
                for key, value in msgs_without_none
            )
            await listener_stream.write(listener_msg)


def _gen_notify_aboute_source_msg(source: store.Source):
    time_since_last_msg = datetime.datetime.now() - source.last_received
    ms_since_last_msg = time_since_last_msg.total_seconds() * 1000.0
    return f'[{source.id_}] {source.serial_num} | {source.state} | {ms_since_last_msg}\r\n'


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
