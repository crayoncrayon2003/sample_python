
# install
```
$ sudo apt update
$ sudo apt install -y software-properties-common
$ sudo add-apt-repository ppa:deadsnakes/ppa
$ sudo apt update

$ sudo apt install -y python3.9   python3.9-venv
$ sudo apt install -y python3.10  python3.10-venv
$ sudo apt install -y python3.11  python3.11-venv
$ sudo apt install -y python3-pip
```

# version
```
$ python -V
$ pip3 -V
```

# swtich version
## Search for python path
```
$ which python3.9
/usr/bin/python3.9

$ which python3.10
/usr/bin/python3.10

$ which python3.11
/usr/bin/python3.11
```

## setting for alternatives
```
update-alternatives --install /usr/local/bin/python python /usr/bin/python3.9 1
update-alternatives --install /usr/local/bin/python python /usr/bin/python3.10 2
update-alternatives --install /usr/local/bin/python python /usr/bin/python3.11 3
```

## swtich ptyhon version
```
update-alternatives --config python
```

# run
## case1
```
$ python filename.py
```

## case2
```
$ python3.10 filename.py
```

