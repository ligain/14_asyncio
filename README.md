# Ycrawler
Yet another simple parser to parse and save news from [Ycombinator](https://news.ycombinator.com) to disk.
It has following options:
| Short option | Long option | Description |
|:---:|:---:|:---:|
| -o | --output-dir | dir where to put parsed data. [default `cwd`] |
| -i | --interval | interval to check the main page in seconds [default 30] |
| -l | --log-dir | dir for logs [default ./ycrawler.log] |

### Run
You should have installed **Python 3.6** on your system.
```
$ git clone https://github.com/ligain/14_asyncio
$ cd 14_asyncio/
$ python3.6 -m venv .env
$ . .env/bin/activate
$ pip install -r requirements.txt
$ python crawler.py
[2018.09.10 22:06:22] I Start crawling from main page: https://news.ycombinator.com
...
```
### Tests
```
$ cd 14_asyncio/
$ python3.6 tests.py
```

### Project Goals
The code is written for educational purposes.