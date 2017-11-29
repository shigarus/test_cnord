import logging

import tornado.iostream
from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer

import config


class Application:

    def __init__(self):
        self._sources_server = SourcesServer(self._on_source_msg)
        self._listener_server = ListenersServer()

    def listen(self, sources_port, listeners_port):
        self._sources_server.listen(sources_port)
        self._listener_server.listen(listeners_port)

    def _on_source_msg(self, source, msg):
        pass


class SourcesServer(TCPServer):

    def __init__(self, on_msg, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._on_msg = on_msg

    async def handle_stream(self, stream, address):
        try:
            while True:
                msg = await stream.read_until_close()
                await self._on_msg(stream, msg)
        except tornado.iostream.StreamClosedError:
            pass


class ListenersServer(TCPServer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._listeners = []

    def handle_stream(self, stream, address):
        self._listeners.append(stream)
        stream.set_close_callback(lambda: self._listeners.remove(stream))

    async def send_msg_to_all(self, msg):
        for listener in tuple(self._listeners):
            await listener.write(msg)


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
