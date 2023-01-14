import threading
import time

from pylogix import PLC


class PlcConn(PLC):
    def __init__(self, keepalive_secs: int = 5, keepalive_tag: str = '', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.keepalive_secs = keepalive_secs
        self.keepalive_tag = keepalive_tag
        self.conn_status: str = ""
        x = threading.Thread(target=self._keepalive, daemon=True)
        x.start()

    def _keepalive(self):
        while True:
            if self.keepalive_tag:
                resp = self.Read(self.keepalive_tag)
                print(f"keepalive executed tag")
            else:
                resp = self.GetPLCTime()
                print(f"keepalive executed plctime")
            self.conn_status = resp.Status
            # print(f"keepalive executed")
            time.sleep(self.keepalive_secs)

    def status(self):
        return self.conn_status
