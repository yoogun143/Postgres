# 1. Set up Tor session in Windows

1. Install [Tor Expert Bundle](https://www.torproject.org/download/tor/) for Windows
2. Extract to a folder location (in my case `C:\Users\thanh\OneDrive\Desktop\tor expert bundle`)
3. Create a new `torrc` file in Tor folder with following content:
```
ControlPort 9051
HashedControlPassword <hashed_password>
```
4. To generate the hashed password, open a **cmd** command 
```cmd
tor --hash-password <your_password>
```
The output will be pasted back into `torrc` file

5. open a **cmd** command in tor folder and open a tor session with path to torcc file
```cmd
tor -f torrc
```

6. Open `tor.py` file and execute command thorough opened tor session

### Note: The structure using Tree Exporter Plugin in VSCode
```
tor expert bundle
├── data
│   ├── geoip
│   └── geoip6
└── tor
    ├── tor-gencert.exe
    ├── tor.exe
    └── torrc
```
