import keyring

service_id = 'IM_YOUR_APP!'
username = 'user'

keyring.delete_password(service_id, username)
