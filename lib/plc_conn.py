import threading
import time

from pylogix import PLC


class PlcConn(PLC):
    # def __init__(self, keepalive_secs: int = 5, keepalive_tag: str = '', *args, **kwargs):
    def __init__(self, keepalive_tag: str = '', *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.keepalive_secs = keepalive_secs
        self.keepalive_tag = keepalive_tag
        # self.conn_status: str = ""
        # x = threading.Thread(target=self._keepalive, daemon=True)
        # x.start()

    def keepalive(self):
        # while True:
        if self.keepalive_tag:
            self.Read(self.keepalive_tag)
            print(f"keepalive executed tag")
        else:
            # resp = self.GetDeviceProperties()
            self.GetPLCTime()
            print(f"keepalive executed plctime")
