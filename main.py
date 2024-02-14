import argparse
import uuid
import time
import datetime

from worker import YggWorkerInterface, get_bus, WORKER_EVENT_NAME_WORKING

"""
Example of yggdrasil worker
"""


class EchoWorker(YggWorkerInterface):
    """The class implementing yggdrasil echo worker."""

    # Name of worker
    _NAME = "echo"

    # Version of worker
    _VERSION = "1"

    def __init__(
            self,
            remote_content: bool = False,
            loop_count: int = 1,
            sleep_time: float = 0
    ) -> None:
        super().__init__(remote_content=remote_content)
        print(f"Created '{self._NAME}' worker: {self}, loop_count: {loop_count}, sleep_time: {sleep_time}")
        self.loop_count = loop_count
        self.sleep_time = sleep_time

    def send_echo_message(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
        """
        Send a message back to the yggd
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return:
        """
        self.emit_signal(WORKER_EVENT_NAME_WORKING, msg_id, response_to, {"message": f"echoing {data}"})
        self.transmit(addr, msg_id, response_to, metadata, data)
        now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
        self.set_feature("DispatchedAt", str(now))

    def rx_handler(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
        """
        Handler of message received over D-Bus from yggdrasil server
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return:
        """
        print(f"Dispatch: addr: '{addr}' msg_id: '{msg_id}' response_to: '{response_to}' "
              f"metadata: {metadata} data: '{data}'")
        for i in range(self.loop_count):
            if self.threads[msg_id].stopped():
                print(f"It was requested to stop {msg_id} message. Canceling this job...")
                break
            print(f"Sending echo message {i}...")
            self.send_echo_message(
                addr=addr,
                msg_id=str(uuid.uuid4()),
                response_to=msg_id,
                metadata=metadata,
                data=data
            )
            if self.sleep_time > 0:
                print(f"Sleeping for {self.sleep_time} seconds...")
                time.sleep(self.sleep_time)
        print("Dispatch done")

    def cancel_handler(self, directive: str, msg_id: str, cancel_id: str) -> None:
        """
        Handler of cancel command received over D-Bus from yggdrasil server
        :param directive:
        :param msg_id:
        :param cancel_id:
        :return:
        """
        print(f"Canceling message: {directive}, {msg_id}, {cancel_id}")
        try:
            self.threads[cancel_id].stop()
        except KeyError:
            print(f"Thread for {cancel_id} does not exists.")


def _main():
    parser = argparse.ArgumentParser(description="Example of echo worker")
    parser.add_argument(
        "--remote-content",
        type=bool,
        default=False,
        help="Connect as a remote content worker"
    )
    parser.add_argument(
        "--loop",
        type=int,
        default=1,
        help="number of loop echoes before finish echoing. (default 1)")
    parser.add_argument(
        "--sleep",
        default=0,
        type=float,
        help="sleep time in seconds before echoing the response")
    args = parser.parse_args()

    try:
        # Create an instance of the class EchoWorker.
        echo_worker = EchoWorker(
            remote_content=args.remote_content,
            loop_count=args.loop,
            sleep_time=args.sleep
        )

        # Not necessary, just nice to print the generated XML specification.
        echo_worker.print_dbus_xml()

        # Start the loop
        echo_worker.connect()
    finally:
        # Unregister the DBus service and objects.
        get_bus().disconnect()


if __name__ == '__main__':
    _main()
