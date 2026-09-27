# install

```bash
$ sudo apt update
$ sudo apt install -y software-properties-common
$ sudo add-apt-repository ppa:deadsnakes/ppa
$ sudo apt update

$ sudo apt install -y python3.12  python3.12-venv
$ sudo apt install -y python3.14  python3.14-venv
$ sudo apt install -y python3-pip
```

# version

```bash
$ python -V
$ pip3 -V
```

# swtich version

## Search for python path

```bash
$ which python3.12
/usr/bin/python3.12

$ which python3.14
/usr/bin/python3.14
```

## setting for alternatives

```bash
sudo update-alternatives --install /usr/local/bin/python python /usr/bin/python3.12 1
sudo update-alternatives --install /usr/local/bin/python python /usr/bin/python3.14 2
```

## swtich ptyhon version

```bash
sudo update-alternatives --config python
```

# run

## case1

```bash
$ python filename.py
```

## case2

```bash
$ python3.12 filename.py
```
