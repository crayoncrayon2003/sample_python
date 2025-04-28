import pyshark
import os

def main():
    ROOT = os.path.dirname(os.path.abspath(__file__))
    FILE = os.path.join(ROOT,"a.pcap")

    cap = pyshark.FileCapture(FILE)
    for pkt in cap:
        print(pkt)

if __name__ == '__main__':
    main()