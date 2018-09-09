import unittest
import re

from bs4 import BeautifulSoup
from crawler import slugify_url, MAX_SLUG_LEN


class TestSlugify(unittest.TestCase):

    def test_url_content_slug(self):
        a_tag = BeautifulSoup('<a href="https://github.com/p-gen/smenu" class="storylink">Smenu, a command-line '
                              'advanced selection [filter] and a menu builder for: terminal!</a>').a
        slug = slugify_url(a_tag)
        self.assertLessEqual(len(slug), MAX_SLUG_LEN)
        self.assertIsNone(re.search(r'[^a-zA-Z0-9_\-]+', slug))

    def test_bad_url_content(self):
        bad_a_tag = '<a href="https://github.com/p-gen/smenu" class="storylink">Smenu, a command-line advanced '\
                    'selection [filter] and a menu builder for: terminal!</a>'
        with self.assertRaises(TypeError):
            slugify_url(bad_a_tag)

    def test_url_href_slug(self):
        a_tag = BeautifulSoup('<a href="https://github.com/p-gen/smenu/"></a>').a
        slug = slugify_url(a_tag, use_href=True)
        self.assertEqual(slug, 'smenu.html')

    def test_too_long_href(self):
        a_tag = BeautifulSoup('<a href="https://jobs.lever.co/rescale/ba8800d3-b0bd-40b0-8a72-887e27904553?lever'
                              '-origin=applied&lever-source%5B%5D=Hacker%20News/"></a>').a
        slug = slugify_url(a_tag, use_href=True)
        self.assertLessEqual(len(slug), MAX_SLUG_LEN + len('.html'))

    def test_url_with_html_ending(self):
        a_tag = BeautifulSoup('<a href="https://www.cia.gov/library/center-for-the-study-of-intelligence/kent-csi'
                              '/vol6no4/html/v06i4a05p_0001.htm"></a>').a
        slug = slugify_url(a_tag, use_href=True)
        self.assertEqual(slug, 'v06i4a05p_0001.htm')


if __name__ == '__main__':
    unittest.main()