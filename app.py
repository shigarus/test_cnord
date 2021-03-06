import datetime
import logging
from typing import Callable, Sequence, Tuple, Any

from tornado.iostream import StreamClosedError, IOStream
from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer

import config
import source_protocol
import store


class Dispatcher:
    """
    Serves as a bridge between sources and listeners.

    Receives msgs from sources and redirects them to listeners.
    Notifies sources about received messages.
    Notifies listeners about existed sources.
    """

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

    async def _on_source_connect(self, source_stream: IOStream):
        pass

    async def _on_source_msg(self, source_stream: IOStream, msg: bytes):
        """
        Every message from sources has to be redirected to listeners.
        And every listener wants to receive a state of every source - so we
        need to update this state every time.
        """
        logging.debug(f'received from source {msg}')
        parsed = source_protocol.parse_source_bytes(msg)
        if not parsed:
            answer_to_source = source_protocol.gen_answer_to_source(success=False)
            await source_stream.write(answer_to_source)
            return
        logging.debug(f'parsed to {parsed}')
        source_id = parsed['source_id']
        store.SourcesStore.update_state(
            source_id=source_id,
            serial_num=parsed['num'],
            state=parsed['source_state'],
            last_received=datetime.datetime.now(),
        )
        if source_id not in self._sources_connects:
            self._sources_connects[source_id] = source_stream
        answer_to_source = source_protocol.gen_answer_to_source(
            success=True,
            serial_num=parsed['num'],
        )
        await source_stream.write(answer_to_source)
        logging.debug(f'sources {source_id} notified with {answer_to_source} ')
        await self._send_to_listeners(parsed['msgs'], source_id)

    async def _on_source_close(self, source_stream: IOStream):
        """
        Need to delete pointer to the stream
        """
        logging.debug('closed source')
        source_id = next(
            (k for k, v in self._sources_connects.items() if v is source_stream),
            None,
        )
        if source_id:
            logging.debug(f'source had id {source_id}')
            self._sources_connects.pop(source_id)

    async def _on_listener_connect(self, listener_stream: IOStream):
        """
        Registers a listener in the system
        """
        id_ = store.ListenersStore.add_listener()
        self._listeners_connects[id_] = listener_stream
        await self._notify_about_sources(id_, listener_stream)

    async def _on_listener_close(self, listener_stream: IOStream):
        """
        Need to delete pointer to the stream
        """
        listener_id = next(
            (k for k, v in self._listeners_connects.items() if v is listener_stream),
            None,
        )
        if listener_id:
            self._listeners_connects.pop(listener_id)

    @staticmethod
    async def _notify_about_sources(listener_id: int, listener_stream: IOStream):
        sources = store.SourcesStore.get_all()
        str_per_source = (
            _gen_notify_about_source_msg(source)
            for source in sources
        )
        logging.debug(f'Listener {listener_id} connected to system')
        await listener_stream.write(b''.join(str_per_source))
        # Need to store notified state, cuz while we notify current sources -
        # more of them can connect.
        # So on every received message we will check if a listener was
        # notified about a source of the message
        for source in sources:
            logging.debug(f'Listener {listener_id} notified about source {source.id_}')
            store.ListenersStore.set_notified(listener_id, source.id_)

    async def _send_to_listeners(self, msgs: Sequence[Tuple[str, int]], source_id):
        for listener in store.ListenersStore.get_all():
            listener_stream = self._listeners_connects[listener.id_]

            # ensure listener knows about source before sending him current messages
            if source_id not in listener.sources_notified:
                source = store.SourcesStore.get_state(source_id)
                await listener_stream.write(_gen_notify_about_source_msg(source))
                logging.debug(f'Listener {listener.id_} notified about source {source_id}')

            listener_msg = b''.join(
                bytes(f'[{source_id}] {str(key, encoding="ascii")} | {value}\r\n', encoding='ascii')
                for key, value in msgs
            )
            await listener_stream.write(listener_msg)
            logging.debug(f'To listener {listener.id_} sent {listener_msg}')


def _gen_notify_about_source_msg(source: store.Source):
    time_since_last_msg = datetime.datetime.now() - source.last_received
    ms_since_last_msg = time_since_last_msg.total_seconds() * 1000.0
    msg = f'[{source.id_}] {source.serial_num} | {source.state} | {ms_since_last_msg}\r\n'
    return bytes(msg, encoding='ascii')


class SourcesServer(TCPServer):
    """
    Manages connects to sources.
    """

    def __init__(self,
                 on_connect: Callable[[IOStream], Any],
                 on_msg: Callable[[IOStream, bytes], Any],
                 on_close: Callable[[IOStream], Any],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_connect = on_connect
        self._on_msg = on_msg
        self._on_close = on_close

    async def handle_stream(self, stream: IOStream, address: str):
        await self._on_connect(stream)
        try:
            while True:
                meta = await stream.read_bytes(13)
                msgs = await stream.read_bytes(meta[-1] * 13)
                await self._on_msg(stream, meta+msgs)
        except StreamClosedError:
            await self._on_close(stream)


class ListenersServer(TCPServer):
    """
    Manages connects to listeners.
    """

    def __init__(self,
                 on_connect: Callable[[IOStream], Any],
                 on_close: Callable[[IOStream], Any],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_connect = on_connect
        self._on_close = on_close

    async def handle_stream(self, stream: IOStream, address: str):
        await self._on_connect(stream)
        try:
            while True:
                # just wait until close - we don't expect any data from a listener
                await stream.read_until_close()
        except StreamClosedError:
            await self._on_close(stream)


def main():
    conf = config.get_config()
    logging.basicConfig(
        level=logging.DEBUG if conf['debug'] else logging.INFO,
        format='%(levelname)s:%(asctime)s:%(message)s',
    )
    disp = Dispatcher()
    disp.listen(
        sources_port=conf['sources_port'],
        listeners_port=conf['listeners_port'],
    )
    IOLoop.current().start()


if __name__ == '__main__':
    main()
