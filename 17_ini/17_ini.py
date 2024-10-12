import os
import configparser

CONFIG = 'TEST'
config_ini = configparser.ConfigParser()
config_ini.read(os.path.join(os.path.dirname(os.path.abspath(__file__)),"config.ini"), encoding='utf-8')

def main():
    print(config_ini[CONFIG]['HOST_IP'])
    print(config_ini[CONFIG]['HOST_PORT'])


if __name__=='__main__':
    main()
