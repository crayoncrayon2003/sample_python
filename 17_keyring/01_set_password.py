import keyring

service_id = 'IM_YOUR_APP!'
username = 'user'
password = 'password'

keyring.set_password(service_id, username, password)
