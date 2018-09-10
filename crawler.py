import argparse
import logging
import os
import re

from aiohttp import ClientSession
import asyncio

from bs4 import BeautifulSoup, Tag

MAIN_PAGE = 'https://news.ycombinator.com'
MAX_SLUG_LEN = 50


async def get_url_content(session: ClientSession, url: str, retries=5):
    while retries > 0:
        try:
            response = await session.get(url)
        except:
            logging.exception(f'An error while getting: {url}. Retrying...')
            retries -= 1
            continue
        else:
            if response.status == 200 and ('text/html' in response.headers.get('content-type')):
                text_content = (await response.read()).decode('utf-8', 'replace')
            else:
                text_content = ''
            response.close()
            return BeautifulSoup(text_content, 'html.parser')


def save_page(filename, dir, content: BeautifulSoup):
    if not content.get_text():
        logging.error(f'Empty content for file: {filename} in dir: {dir}')
        return
    fullpath = os.path.join(dir, filename)
    with open(fullpath, 'w') as file:
        file.write(str(content))


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
    return parser.parse_args()


async def process_comment_url(session: ClientSession, url: Tag, output_dir):
    content = await get_url_content(session, url.get('href'))
    logging.info(f"Comment content: {content.get_text()[:100]}")
    comment_url_filename = slugify_url(url, use_href=True)
    logging.info(f"Save comment url in file: {comment_url_filename} in folder: {output_dir}")
    save_page(comment_url_filename, output_dir, content)


async def process_news_url(session: ClientSession, url: Tag, output_dir):
    if not url.get('href').startswith('http'):
        comment_page_full_url = os.path.join(MAIN_PAGE, url.get('href'))
    else:
        comment_page_full_url = url.get('href')
    logging.info(f"Parsing content of page: {comment_page_full_url}")
    comment_page_content = await get_url_content(session, comment_page_full_url)

    news_url = comment_page_content.select('td.title > a')[0]
    news_content = await get_url_content(session, news_url.get('href'))
    news_folder_name = slugify_url(news_url)
    news_full_path = os.path.join(output_dir, news_folder_name)
    if not os.path.exists(news_full_path):
        logging.info(f"Create new folder: {news_folder_name} for a piece of news")
        os.mkdir(news_full_path)

        # save article page
        logging.info(f"Save article.html in folder: {news_folder_name}")
        save_page('article.html', news_full_path, news_content)

        # save comments page
        logging.info(f"Save comments of page: {comment_page_full_url} in file: comments.html")
        save_page('comments.html', news_full_path, comment_page_content)

        # save comments urls
        comments_urls = comment_page_content.select('div.comment a[rel="nofollow"]')
        for comment_url in comments_urls:
            await process_comment_url(session, comment_url, news_full_path)
    else:
        logging.info(f"News page: {news_url.get('href')} already exists in output dir. Skipping...")


async def main(url, args):
    logging.info(f'Start crawling from main page: {url}')

    while True:
        async with ClientSession(conn_timeout=60) as session:
            main_page = await get_url_content(session, url)
            comments_links = main_page.select('td.subtext > a:nth-of-type(3)')
            for comment_link in comments_links:
                await process_news_url(session, comment_link, args.output_dir)
        logging.info('Crawling is finished')
        await asyncio.sleep(args.interval)


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
