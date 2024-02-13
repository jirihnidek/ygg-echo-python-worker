import os
from typing import Optional
from dasbus.typing import Str, Dict, Bool, UInt32, List, Byte
from dasbus.server.interface import dbus_interface
from dasbus.loop import EventLoop
from dasbus.server.template import InterfaceTemplate
from dasbus.server.interface import dbus_signal
from dasbus.connection import SessionMessageBus, SystemMessageBus
from dasbus.signal import Signal
from dasbus.identifier import DBusServiceIdentifier
from dasbus.xml import XMLGenerator
import threading
import gi

"""
This module contain base class for implementation yggdrasil worker in pure Python
"""

gi.require_version("GLib", "2.0")

# Constants of events
WORKER_EVENT_NAME_BEGIN = 1
WORKER_EVENT_NAME_END = 2
WORKER_EVENT_NAME_WORKING = 3
WORKER_EVENT_NAME_STARTED = 4
WORKER_EVENT_NAME_STOPPED = 5


def get_bus():
    """
    Get message bus.

    It could be session bus, when environment variable
    DBUS_SESSION_BUS_ADDRESS is defined. Otherwise, system bus is returned.
    :return: Instance of message bus
    """
    if "DBUS_SESSION_BUS_ADDRESS" in os.environ:
        print(f"Using session bus {os.environ['DBUS_SESSION_BUS_ADDRESS']}")
        bus = SessionMessageBus()
    else:
        print("Using system bus")
        bus = SystemMessageBus()
    return bus


class YggWorkerThread(threading.Thread):
    """
    Custom thread for ygg worker
    """
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._stop_even = threading.Event()

    def stop(self) -> None:
        self._stop_even.set()

    def stopped(self) -> bool:
        return self._stop_even.is_set()


# Define services and objects.
YGG_WORKER = DBusServiceIdentifier(
    namespace=("com", "redhat", "Yggdrasil1", "Worker1"),
    message_bus=get_bus()
)


@dbus_interface(YGG_WORKER.interface_name)
class YggWorkerInterface(InterfaceTemplate):
    """The DBus interface for yggdrasil worker."""

    _VERSION = "1"
    _NAME = "yggdrasil_worker"

    def __init__(self, remote_content: bool):
        """
        Initialize the YggWorker object
        """
        self.event_signal: Signal = Signal()
        super().__init__(self)
        self.remote_content: bool = remote_content
        self.features: dict = {
            "DispatchedAt": "",
            "Version": self._VERSION
        }
        self.threads: dict[str, YggWorkerThread] = {}
        self.main_loop: Optional[EventLoop] = None
        self.bus = None

    def print_dbus_xml(self) -> None:
        """
        Print the DBus XML to stdout
        :return:
        """
        print(XMLGenerator.prettify_xml(self.__dbus_xml__))

    def emit_signal(self, signal_name: int, message_id: str, response_to: str, data: dict) -> None:
        """
        Emit "Event" signal over D-Bus
        :return: None
        """
        self.event_signal.emit(
            signal_name,
            message_id,
            response_to,
            data
        )

    def connect(self) -> None:
        """
        Connect to the yggdrasil server and start the main loop
        :return:
        """
        self.bus = get_bus()
        self.bus.publish_object(YGG_WORKER.object_path + "/" + self._NAME, self)
        self.bus.register_service(YGG_WORKER.service_name + "." + self._NAME)

        self.emit_signal(
            WORKER_EVENT_NAME_STARTED,
            "",
            "",
            {}
        )

        # Start the event loop.
        self.main_loop = EventLoop()
        try:
            self.main_loop.run()
        finally:
            self.emit_signal(
                WORKER_EVENT_NAME_STOPPED,
                "",
                "",
                {}
            )

    def set_feature(self, name: str, value: str) -> None:
        """
        Try to set feature
        :param name: The name of the feature
        :param value: New value of the feature
        :return: None
        """
        # FIXME: emit D-Bus signal that property has changed
        self.features[name] = value

    @property
    def RemoteContent(self) -> Bool:
        """The RemoteContent property."""
        return self.remote_content

    @property
    def Features(self) -> Dict[str, str]:
        """
        Features property returning dictionary with the last time, when the
        worker was dispatched and version of the worker
        """
        return self.features

    def event_handler(self, signal_name: int, message_id: str, response_to: str, data: dict) -> None:
        raise NotImplementedError

    def connect_signals(self):
        """Connect the signals."""
        self.event_signal.connect(self.Event)

    @dbus_signal
    def Event(self, signal_name: UInt32, message_id: Str, response_to: Str, data: Dict[Str, Str]) -> None:
        """
        Handler of Event signal
        :param signal_name:
        :param message_id:
        :param response_to:
        :param data:
        :return: None
        """
        self.event_handler(signal_name, message_id, response_to, data)

    def transmit(self, addr: Str, msg_id: Str, response_to: Str, metadata: Dict[Str, Str], data: List[Byte]) -> None:
        """
        Transmit method
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return: None
        """
        print("Transmitting", self, addr, msg_id, response_to, metadata, data)
        proxy = self.bus.get_proxy(
            service_name="com.redhat.Yggdrasil1.Dispatcher1",
            object_path="/com/redhat/Yggdrasil1/Dispatcher1",
            interface_name="com.redhat.Yggdrasil1.Dispatcher1"
        )

        def callback(call):
            print(f"Calling transmit callback: {call}")
            response_code, response_metadata, response_data = call()
            print(f"Transmit callback result: {response_code}, {response_metadata}, {response_data}")

        proxy.Transmit(addr, msg_id, response_to, metadata, data, callback=callback)

    def rx_handler(self, *args, **kwargs) -> None:
        """
        Receive callback method
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError

    def dispatch_handler(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
        """
        Handler of dispatched message
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return: None
        """
        self.emit_signal(
            WORKER_EVENT_NAME_BEGIN,
            msg_id,
            response_to,
            {}
        )
        self.rx_handler(addr, msg_id, response_to, metadata, data)
        self.emit_signal(
            WORKER_EVENT_NAME_END,
            msg_id,
            response_to,
            {}
        )
        del self.threads[msg_id]

    def Dispatch(self, addr: Str, msg_id: Str, response_to: Str, metadata: Dict[Str, Str], data: List[Byte]) -> None:
        """
        This method is called by yggdrasil service, when a message is received by yggdrasil
        and is dispatched to this worker
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return: None
        """

        thread = YggWorkerThread(
            target=self.dispatch_handler,
            args=[addr, msg_id, response_to, metadata, data]
        )
        thread.daemon = True
        thread.start()
        self.threads[msg_id] = thread

    def cancel_handler(self, directive: str, msg_id: str, cancel_id: str) -> None:
        raise NotImplementedError

    def Cancel(self, directive: Str, msg_id: Str, cancel_id: Str) -> None:
        """
        This method is called when a cancel command is received by yggdrasil server
        for message  with given cancel_id
        :param directive:
        :param msg_id:
        :param cancel_id:
        :return:
        """
        self.cancel_handler(directive, msg_id, cancel_id)
