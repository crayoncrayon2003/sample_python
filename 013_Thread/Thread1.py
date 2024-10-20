import threading
import time

class devices(threading.Thread):
    mtx  = threading.Lock()
    Value      :int
    isThreading:bool

    def __init__(self):
        self.setValue(0)
        self.isThreading = True

        super(devices, self).__init__()

    def stop(self):
        self.isThreading = False

    def run(self):
        while(self.isThreading==True):
            print('value {} :'.format(self.getValue()))
            time.sleep(1)
            self.upValue()

    def upValue(self):
        self.mtx.acquire()
        self.Value += 1
        self.mtx.release()

    def setValue(self,Value):
        self.mtx.acquire()
        self.Value = Value
        self.mtx.release()

    def getValue(self):
        self.mtx.acquire()
        value = self.Value
        self.mtx.release()
        return value

if __name__ == '__main__':
    testdevice = devices()
    testdevice.start()
    time.sleep(6)
    testdevice.setValue(0)
    time.sleep(6)
    testdevice.setValue(0)
    time.sleep(6)
    testdevice.stop()


