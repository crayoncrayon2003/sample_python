import pyshark

def main():
    cap = pyshark.LiveCapture(interface='eth0')
    for pkt in cap:
        print(pkt)

if __name__ == '__main__':
    main()