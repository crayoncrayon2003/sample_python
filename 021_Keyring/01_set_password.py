import keyring
import getpass

service_id = 'IM_YOUR_APP!'
username = 'user'
password = 'password'

# username = input("input username : ")
# password = getpass.getpass("input password : ")

keyring.set_password(service_id, username, password)
