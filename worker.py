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
import logging
from multiprocessing import Process, Queue
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


log = logging.getLogger(__name__)


def _get_bus():
    """
    Get message bus.

    It could be session bus, when environment variable
    DBUS_SESSION_BUS_ADDRESS is defined. Otherwise, system bus is returned.
    :return: Instance of message bus
    """
    if "DBUS_SESSION_BUS_ADDRESS" in os.environ:
        log.debug(f"using session bus {os.environ['DBUS_SESSION_BUS_ADDRESS']}")
        bus = SessionMessageBus()
    else:
        log.debug("using system bus")
        bus = SystemMessageBus()
    return bus


MESSAGE_BUS = _get_bus()

YGG_WORKER_NAMESPACE = ("com", "redhat", "Yggdrasil1", "Worker1")
YGG_WORKER_INTERFACE_NAME = get_dbus_name(*YGG_WORKER_NAMESPACE)


class _YggJob:
    """
    Class representing job of yggdrasil worker. This class encapsulates one process
    and one thread. We cannot use only process for job, because dasbus expects that
    threads (not more processes) are used for calling dasbus API. Communication
    between job thread and process is done via queue.
    """

    def __init__(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
        """
        Constructor for YggJob holding information about current job triggered by one MQTT message
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        """
        self._addr = addr
        self._msg_id = msg_id
        self._response_to = response_to
        self._metadata = metadata
        self._data = data
        self.thread: Optional[threading.Thread] = None
        self.process: Optional[Process] = None
        self.queue: Optional[Queue] = None

    def start(self, thread_handler) -> None:
        """
        Try to start thread and process for this job
        :return: None
        """
        self.queue = Queue()
        self.thread = threading.Thread(
            target=thread_handler,
            name=self._msg_id,
            args=[self._addr, self._msg_id, self._response_to, self._metadata, self._data]
        )
        # self.thread.name = self._msg_id
        self.thread.daemon = True
        self.thread.start()

    def terminate(self) -> None:
        """
        Try to terminate corresponding process and thread
        :return: None
        """
        if self.process is not None:
            self.process.terminate()

    def kill(self) -> None:
        """
        Try to kill corresponding process and try to stop thread
        :return: None
        """
        if self.process is not None:
            self.process.kill()


@dbus_interface(YGG_WORKER_INTERFACE_NAME)
class YggWorker(InterfaceTemplate):
    """
    Base class and DBus interface for yggdrasil worker.

    Usage:

    from worker import YggWorker, MESSAGE_BUS

    class ExampleWorker(YggWorker):

        _NAME = "example"

        def __init__(self, remote_content: bool = False) -> None:
            super().__init__(remote_content=remote_content)

        def dispatch_handler(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
            # Do something useful here
            pass

        def cancel_handler(self, directive: str, msg_id: str, cancel_id: str) -> None:
            # YggWorkerInterface uses threads for jobs. It is possible to cancel job using:
            try:
                self.terminate(cancel_id)
            except KeyError:
                print(f"Job for {cancel_id} does not exist.")

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
        self._jobs: dict[str, _YggJob] = {}
        self.main_loop: Optional[EventLoop] = None
        self.namespace = None

    def print_dbus_xml(self) -> None:
        """
        Print the DBus XML to stdout
        :return: None
        """
        log.debug(XMLGenerator.prettify_xml(self.__dbus_xml__))

    def emit_signal(self, signal_name: int, message_id: str, response_to: str, data: dict) -> None:
        """
        Emit "Event" signal over D-Bus
        :return: None
        """
        log.debug(f"emitting signal: {signal_name}, {message_id}, {response_to}, {data}")
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

    @staticmethod
    def _transmit(addr: str, msg_id: str, response_to: str, metadata: Dict[str, str], data: List[Byte]) -> None:
        """
        Damn, I do not like Python
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return:
        """
        log.debug(f"transmitting {addr}, {msg_id}, {response_to}, {metadata}, {data}")
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
            log.debug(f"calling transmit callback: {call}")
            response_code, response_metadata, response_data = call()
            log.debug(f"transmit callback result: {response_code}, {response_metadata}, {response_data}")

        proxy.Transmit(addr, msg_id, response_to, metadata, data, callback=callback)

    def transmit(self, addr: str, msg_id: str, response_to: str, metadata: Dict[str, str], data: List[Byte]) -> None:
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
        log.debug(f"putting data of message {msg_id} to queue for transmit")
        self._jobs[response_to].queue.put((addr, msg_id, response_to, metadata, data))

    def terminate_job(self, msg_id: str) -> None:
        """
        Try to terminate job
        :param msg_id: ID of job to be terminated
        :return: None
        """
        try:
            log.debug(f"terminating job {msg_id}")
            self._jobs[msg_id].terminate()
        except KeyError:
            log.debug(f"Job with message id: {msg_id} does not exist")
        else:
            log.debug(f"Job {msg_id} terminated")

    def kill_job(self, msg_id: str) -> None:
        """
        Try to kill job. Kill could not be masked.
        :param msg_id: ID of job to be killed
        :return: None
        """
        try:
            log.debug(f"terminating job {msg_id}")
            self._jobs[msg_id].kill()
        except KeyError:
            log.debug(f"Job with message id: {msg_id} does not exist")
        else:
            log.debug(f"Job {msg_id} killed")

    def dispatch_handler(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
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
        self._jobs[msg_id].process = Process(
            target=self.dispatch_handler,
            args=[addr, msg_id, response_to, metadata, data],
            daemon=True
        )
        self._jobs[msg_id].process.start()
        # Wait in queue for end of process.
        # Should we define any timeout for the process?
        while self._jobs[msg_id].process.is_alive() and self._jobs[msg_id].process.join(timeout=None):
            resp_addr, resp_msg_id, resp_response_to, resp_metadata, resp_data = self._jobs[msg_id].queue.get(block=True)
            self._transmit(
                addr=resp_addr,
                msg_id=resp_msg_id,
                response_to=resp_response_to,
                metadata=resp_metadata,
                data=resp_data
            )
        del self._jobs[msg_id]
        self.emit_signal(
            WORKER_EVENT_NAME_END,
            msg_id,
            response_to,
            {}
        )

    def Dispatch(self, addr: Str, msg_id: Str, response_to: Str, metadata: Dict[Str, Str], data: List[Byte]) -> None:
        """
        This D-Bus method is called by yggdrasil service, when a message is received by yggdrasil
        and is dispatched to this worker
        :param addr: Unique string representing worker (self._NAME)
        :param msg_id: UUID representing message
        :param response_to: UUID of the message responsible for
        :param metadata: Dictionary containing metadata
        :param data: Raw data of the message
        :return: None
        """

        log.debug(f"dispatching message {msg_id}")
        self._jobs[msg_id] = _YggJob(addr, msg_id, response_to, metadata, data)
        # Start process and thread here and do not wait for thread or process to join,
        # because it would block dispatching of other messages
        self._jobs[msg_id].start(thread_handler=self._dispatch_handler)

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
        This D-Bus method is called by yggdrasil service, when a cancel command is received by yggdrasil
        for message with given cancel_id
        :param directive: Unique string representing worker (self._NAME)
        :param msg_id: UUID of the message
        :param cancel_id: UUID of the message that should be canceled
        :return: None
        """
        self.cancel_handler(directive, msg_id, cancel_id)
