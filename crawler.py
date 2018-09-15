import argparse
import asyncio
import logging
import os
import multiprocessing
import re
import time

from concurrent.futures import ThreadPoolExecutor
from aiohttp import ClientSession
from bs4 import BeautifulSoup, Tag


MAIN_PAGE = 'https://news.ycombinator.com'
MAX_SLUG_LEN = 50
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) ""AppleWebKit 537.36 (KHTML, like Gecko) Chrome",
    "Accept": "text/html,application/xhtml+xml,""application/xml;q=0.9,image/webp,*/*;q=0.8"
}


def get_args():
    parser = argparse.ArgumentParser(
        description='News Ycombinator parser'
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
        default=30,
        type=int
    )
    parser.add_argument(
        '-l', '--log-dir',
        help='dir for logs [default ./ycrawler.log]',
        default=os.path.join(os.getcwd(), 'ycrawler.log'),
        metavar='log_dir'
    )
    parser.add_argument(
        '-d', '--debug',
        help='turn on debug mode',
        action='store_true'
    )
    return parser.parse_args()


async def get_url_content(session: ClientSession, url: str, retries=5, retry_timeout=2, logger=None):
    text_content = ''
    if not logger:
        logger = logging
    while retries > 0:
        try:
            response = await session.get(url)
        except asyncio.CancelledError:
            logger.error(f'Fetching url: {url} was canceled')
        except:
            logger.exception(f'An error while getting: {url}. Retrying...')
            retries -= 1
            await asyncio.sleep(retry_timeout)
            continue
        else:
            if response.status == 200 and ('text/html' in response.headers.get('content-type')):
                text_content = (await response.read()).decode('utf-8', 'replace')
            elif response.status >= 400:
                logger.error(f"Received {response.status} error on getting url: {url} Retrying...")
                await asyncio.sleep(retry_timeout)
                continue
            response.close()
            return BeautifulSoup(text_content, 'html.parser')


def slugify_url(url: Tag, use_href=False):
    if not isinstance(url, Tag):
        raise TypeError('url should have type <bs4.element.Tag>')
    result = ''
    if use_href:
        link_href_text = url.get('href').lower().rstrip('/')
        if link_href_text.endswith('.html') or link_href_text.endswith('.htm'):
            result = link_href_text.rpartition('/')[2]
        else:
            cleaned_href = link_href_text.rpartition('/')[2][:MAX_SLUG_LEN]
            result = f'{cleaned_href}.html'
    else:
        link_content_text = url.get_text().lower().strip()
        result = re.sub(r'(\W|\s)+', '-', link_content_text)[:MAX_SLUG_LEN]
    return result


def make_full_url(prefix: str, path: Tag):
    if not path.get('href').startswith('http'):
        return os.path.join(prefix, path.get('href'))
    return path.get('href')


class Downloader:
    def __init__(self, loop, download_queue, options,
                 timeout=30, headers=None, empty_queue_timeout=1, logger=None):
        self.loop = loop
        self.download_queue = download_queue
        self.options = options
        self.timeout = timeout
        self.empty_queue_timeout = empty_queue_timeout
        self.log = logger if logger else logging
        self.session = ClientSession(loop=self.loop, headers=headers)
        self.pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())

    @staticmethod
    def save_file(fullpath, content: BeautifulSoup):
        with open(fullpath, 'w') as file:
            file.write(str(content))

    async def save_page(self, url, dir, filename, content: BeautifulSoup):
        fullpath = os.path.join(dir, filename)
        self.log.info(f"Saving content from url: {url} to path: {fullpath}")
        try:
            await self.loop.run_in_executor(self.pool, self.save_file, fullpath, content)
        except OSError:
            self.log.exception(f"Error on saving to filepath: {fullpath}")

    async def get_content(self, url):
        content = None
        try:
            content = await asyncio.wait_for(
                get_url_content(self.session, url, logger=self.log),
                timeout=self.timeout,
                loop=self.loop
            )
        except asyncio.TimeoutError:
            self.log.error(f"Timeout reached for url: {url}")
        except Exception:
            self.log.exception(f"Error on fetching url: {url}")
        return content

    async def run(self):
        self.log.info(f'Started downloader worker')
        async with self.session:
            while True:
                while not self.download_queue.empty():
                    item = await self.download_queue.get()
                    filename, dir, url = item
                    content = await self.get_content(url)
                    if not content or not content.get_text():
                        logging.error(f'Empty content for file: {filename} in dir: {dir}')
                        continue
                    await self.save_page(url, dir, filename, content)
                    self.download_queue.task_done()
                    self.log.info(f'Processed url: {url} from download queue')
                await asyncio.sleep(self.empty_queue_timeout)
                self.log.info(f'Download queue is empty '
                              f'sleep for {self.empty_queue_timeout} sec')


class Ycrawler:
    news_url_selector = 'td.subtext span.age a'
    url_in_comment_page_selector = 'td.title > a'
    urls_in_comments = 'div.comment a[rel="nofollow"]'

    def __init__(self, main_url, loop, download_queue, options, headers=None, logger=None):
        self.main_url = main_url
        self.loop = loop
        self.options = options
        self.download_queue = download_queue
        self.log = logger if logger else logging
        self.session = ClientSession(loop=self.loop, headers=headers)
        self.news_tasks = []
        self.processed_news_urls = set()

    async def process_comment_url(self, url: Tag, output_dir):
        comment_url_filename = slugify_url(url, use_href=True)
        await self.download_queue.put((comment_url_filename, output_dir, url.get('href')))
        self.log.info(f'put url: {url} to download queue')

    async def process_news_url(self, url: Tag):
        comment_page_full_url = make_full_url(self.main_url, url)

        self.log.info(f"Parsing content of comments page: {comment_page_full_url}")
        comment_page_content = await get_url_content(self.session, comment_page_full_url, logger=self.log)
        if not comment_page_content or not comment_page_content.get_text():
            self.log.error(f'Empty content of comments page: {comment_page_full_url}')
            return

        news_url_tag = comment_page_content.select(self.url_in_comment_page_selector)[0]
        news_folder_name = slugify_url(news_url_tag)
        news_url = make_full_url(self.main_url, news_url_tag)
        news_full_path = os.path.join(self.options.output_dir, news_folder_name)
        if not os.path.exists(news_full_path):
            self.log.info(f"Create new folder: {news_folder_name} for a piece of news")
            os.mkdir(news_full_path)

            # save news page
            await self.download_queue.put(('article.html', news_full_path, news_url))
            self.log.info(f'put url: {news_url} to download queue')

            # save comments page
            await self.download_queue.put(('comments.html', news_full_path, comment_page_full_url))
            self.log.info(f'put url: {comment_page_full_url} to download queue')

            # save urls in comments
            comments_urls = comment_page_content.select(self.urls_in_comments)
            for comment_url in comments_urls:
                self.loop.create_task(self.process_comment_url(comment_url, news_full_path))

    async def run(self):
        self.log.info(f'Started crawling worker')
        async with self.session:
            while True:
                start = time.perf_counter()
                self.news_tasks.clear()
                self.log.info(f'Start crawling from main page: {self.main_url}')
                main_page = await get_url_content(self.session, self.main_url, logger=self.log)
                if not main_page or not main_page.get_text():
                    self.log.error(f'Can not fetch main page: {self.main_url}')
                comments_links = main_page.select(self.news_url_selector)
                for comment_link in comments_links:
                    if comment_link.get('href') not in self.processed_news_urls:
                        self.news_tasks.append(
                            self.process_news_url(comment_link)
                        )
                        self.processed_news_urls.add(comment_link.get('href'))
                if self.news_tasks:
                    await asyncio.wait(self.news_tasks, loop=self.loop)
                end = time.perf_counter()
                self.log.info(f'Crawling is finished in: {end - start} seconds. '
                              f'Fetched urls: {len(self.news_tasks)}')
                await asyncio.sleep(self.options.interval)


if __name__ == '__main__':
    args = get_args()
    logging.basicConfig(filename=args.log_dir, level=logging.INFO,
                        format='[%(asctime)s] %(funcName)s: %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    logger = logging.getLogger('asyncio')

    loop = asyncio.get_event_loop()
    download_queue = asyncio.Queue(3, loop=loop)
    crawler = Ycrawler(
        main_url=MAIN_PAGE,
        loop=loop,
        download_queue=download_queue,
        logger=logger,
        options=args,
        headers=HEADERS
    )
    downloader = Downloader(
        loop=loop,
        download_queue=download_queue,
        logger=logger,
        options=args,
        headers=HEADERS
    )
    if args.debug:
        loop.set_debug(enabled=True)
    try:
        loop.run_until_complete(
            asyncio.gather(
                crawler.run(),
                downloader.run()
            )
        )
    except:
        logging.exception("An error: ")
