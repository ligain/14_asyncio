import argparse
import logging
import os
import aiohttp
import asyncio

from bs4 import BeautifulSoup

MAIN_PAGE = 'https://news.ycombinator.com'


async def get_url_content(session, url):
    async with session.get(url) as response:
        text_content = await response.text()
    return BeautifulSoup(text_content, "html.parser")


def save_page(filename, dir, content):
    fullpath = os.path.join(dir, filename)
    with open(fullpath, 'w') as file:
        file.write(str(content))


def get_args():
    parser = argparse.ArgumentParser(
        description="News Ycombinator parser"
    )
    parser.add_argument(
        '-t', '--threads',
        help='number of crawler threads [default 5]',
        type=int,
        default=5
    )
    parser.add_argument(
        '-o', '--output-dir',
        help='where to put parsed data [default .]',
        default=os.getcwd(),
        metavar='output_dir'
    )
    parser.add_argument(
        '-i', '--interval',
        help='interval to check the main page in seconds [default 30]',
        default=10,
        type=int
    )
    parser.add_argument(
        '-l', '--log-dir',
        help='dir for logs [default ./ycrawler.log]',
        default=os.path.join(os.getcwd(), 'ycrawler.log'),
        metavar='log_dir'
    )
    return parser.parse_args()


async def main(url, args):
    logging.info(f'Start crawling from main page: {url}')
    # while True:
    async with aiohttp.ClientSession(conn_timeout=60) as session:
        main_page = await get_url_content(session, url)
        story_links = main_page.find_all('a', class_='storylink')
        print(story_links)
        save_page('sdf.html', '.', main_page)
        # logging.info('parse')
            # await asyncio.sleep(args.interval)


if __name__ == '__main__':
    args = get_args()
    logging.basicConfig(filename=args.log_dir, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    logging.getLogger('asyncio')
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(MAIN_PAGE, args))
    except:
        logging.exception("An error: ")
