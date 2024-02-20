import os
from typing import Optional
from dasbus.typing import Str, Dict, Bool, UInt32, List, Byte
from dasbus.server.interface import dbus_interface
from dasbus.loop import EventLoop
from dasbus.server.template import InterfaceTemplate
from dasbus.server.interface import dbus_signal
from dasbus.server.property import emits_properties_changed
from dasbus.connection import SessionMessageBus, SystemMessageBus
from dasbus.namespace import get_dbus_name, get_dbus_path
from dasbus.signal import Signal
from dasbus.xml import XMLGenerator
import threading
import gi

"""
This module contain base class for implementation yggdrasil worker in pure Python.
"""

gi.require_version("GLib", "2.0")

# Constants of events
WORKER_EVENT_NAME_BEGIN = 1
WORKER_EVENT_NAME_END = 2
WORKER_EVENT_NAME_WORKING = 3
WORKER_EVENT_NAME_STARTED = 4
WORKER_EVENT_NAME_STOPPED = 5


def _get_bus():
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


MESSAGE_BUS = _get_bus()

YGG_WORKER_NAMESPACE = ("com", "redhat", "Yggdrasil1", "Worker1")
YGG_WORKER_INTERFACE_NAME = get_dbus_name(*YGG_WORKER_NAMESPACE)


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


@dbus_interface(YGG_WORKER_INTERFACE_NAME)
class YggWorkerInterface(InterfaceTemplate):
    """
    Base class and DBus interface for yggdrasil worker.

    Usage:

    from worker import YggWorkerInterface, MESSAGE_BUS

    class ExampleWorker(YggWorkerInterface):

        _NAME = "example"

        def __init__(self, remote_content: bool = False) -> None:
            super().__init__(remote_content=remote_content)

        def rx_handler(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
            # Do something useful here
            pass

        def cancel_handler(self, directive: str, msg_id: str, cancel_id: str) -> None:
            # YggWorkerInterface uses threads for jobs. It is possible to cancel job using:
            try:
                self.threads[cancel_id].stop()
            except KeyError:
                print(f"Thread for {cancel_id} does not exist.")

    if __name__ == '__main__':
        try
            example_worker = ExampleWorker()
            example_worker.connect()
        finally:
            MESSAGE_BUS.disconnect()
    """

    # Version of worker
    _VERSION: str = "1"

    # The None should be replaced with some unique string
    _NAME: Optional[str] = None

    def __init__(self, remote_content: bool):
        """
        Initialize the YggWorker object
        :param remote_content: Whether the worker should be connected as a remote content worker
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
        self.namespace = None

    def print_dbus_xml(self) -> None:
        """
        Print the DBus XML to stdout
        :return: None
        """
        print(XMLGenerator.prettify_xml(self.__dbus_xml__))

    def emit_signal(self, signal_name: int, message_id: str, response_to: str, data: dict) -> None:
        """
        Emit "Event" signal over D-Bus
        :return: None
        """
        print(f"Emitting signal: {signal_name}, {message_id}, {response_to}, {data}")
        self.event_signal.emit(
            signal_name,
            message_id,
            response_to,
            data
        )

    def connect(self) -> None:
        """
        Connect to the yggdrasil server and start the main loop
        :return: None
        """
        self.namespace = (*YGG_WORKER_NAMESPACE, self._NAME)
        MESSAGE_BUS.publish_object(get_dbus_path(*self.namespace), self)
        MESSAGE_BUS.register_service(get_dbus_name(*self.namespace))

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

    @emits_properties_changed
    def set_feature(self, name: str, value: str) -> None:
        """
        Try to set feature
        :param name: The name of the feature
        :param value: New value of the feature
        :return: None
        """
        self.features[name] = value
        self.report_changed_property("Features")

    @property
    def RemoteContent(self) -> Bool:
        """
        The RemoteContent property.
        :return: True if the worker works as a remote content worker
        """
        return self.remote_content

    @property
    def Features(self) -> Dict[str, str]:
        """
        Features property returning dictionary with the last time, when the
        worker was dispatched and version of the worker
        :return: Dictionary of feature properties
        """
        return self.features

    def connect_signals(self) -> None:
        """
        Connect the signals.
        :return: None
        """
        self.event_signal.connect(self.Event)

    @dbus_signal
    def Event(self, signal_name: UInt32, message_id: Str, response_to: Str, data: Dict[Str, Str]) -> None:
        """
        Just definition of D-Bus signal. This method is never called.
        :param signal_name: Numeric constant representing signal
        :param message_id: UUID of the message signal is associated with
        :param response_to: UUID of the response contained in the message
        :param data: Dictionary with data
        :return: None
        """
        pass

    def transmit(self, addr: Str, msg_id: Str, response_to: Str, metadata: Dict[Str, Str], data: List[Byte]) -> None:
        """
        Transmit method that could be called from subclass implementing worker.
        This method send data to yggdrasil via D-Bus.
        :param addr: Unique string representation of the worker
        :param msg_id: UUID of the message send to yggdrasil
        :param response_to: UUID of the message worker is responding to
        :param metadata: Metadata
        :param data: Raw data
        :return: None
        """
        print("Transmitting", self, addr, msg_id, response_to, metadata, data)
        proxy = MESSAGE_BUS.get_proxy(
            service_name="com.redhat.Yggdrasil1.Dispatcher1",
            object_path="/com/redhat/Yggdrasil1/Dispatcher1",
            interface_name="com.redhat.Yggdrasil1.Dispatcher1"
        )

        def callback(call) -> None:
            """
            Callback method
            :param call: Function returning result of asynchronous call
            :return: None
            """
            print(f"Calling transmit callback: {call}")
            response_code, response_metadata, response_data = call()
            print(f"Transmit callback result: {response_code}, {response_metadata}, {response_data}")

        proxy.Transmit(addr, msg_id, response_to, metadata, data, callback=callback)

    def rx_handler(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
        """
        Callback method called, when message is received from the yggdrasil
        :param addr: Unique string representing worker (self._NAME)
        :param msg_id: UUID of the message send to yggdrasil
        :param response_to: UUID of the message worker is responding to
        :param metadata: Dictionary with metadata
        :param data: Raw data
        :return: None
        """
        raise NotImplementedError

    def _dispatch_handler(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
        """
        Thread target used for dispatching message
        :param addr: Unique string representing worker (self._NAME)
        :param msg_id: UUID of the message send to yggdrasil
        :param response_to: UUID of the message worker is responding to
        :param metadata: Metadata
        :param data: Raw data
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
        :param addr: Unique string representing worker (self._NAME)
        :param msg_id: UUID representing message
        :param response_to: UUID of the message responsible for
        :param metadata: Dictionary containing metadata
        :param data: Raw data of the message
        :return: None
        """

        thread = YggWorkerThread(
            target=self._dispatch_handler,
            args=[addr, msg_id, response_to, metadata, data]
        )
        thread.daemon = True
        self.threads[msg_id] = thread
        thread.start()

    def cancel_handler(self, directive: str, msg_id: str, cancel_id: str) -> None:
        """
        Callback method called, when cancel command is received from the yggdrasil.
        :param directive: Unique string representing worker (self._NAME)
        :param msg_id: UUID of the message
        :param cancel_id: UUID of the message that should be canceled
        :return: None
        """
        raise NotImplementedError

    def Cancel(self, directive: Str, msg_id: Str, cancel_id: Str) -> None:
        """
        This method is called when a cancel command is received from yggdrasil server
        for message with given cancel_id
        :param directive: Unique string representing worker (self._NAME)
        :param msg_id: UUID of the message
        :param cancel_id: UUID of the message that should be canceled
        :return: None
        """
        self.cancel_handler(directive, msg_id, cancel_id)
