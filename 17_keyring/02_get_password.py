import keyring

service_id = 'IM_YOUR_APP!'
username = 'user'
password = keyring.get_password(service_id, username)

print('username : ', username)
print('password : ', password)
