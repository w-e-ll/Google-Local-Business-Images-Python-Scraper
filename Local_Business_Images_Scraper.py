import os
import sys
import socket
import urllib
import psycopg2
import random
import time
import socks

from sockshandler import SocksiPyHandler
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen
from lxml import html
from lxml.html import fromstring
# from fake_useragent import UserAgent

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys

# timeout in seconds
timeout = 20
socket.setdefaulttimeout(timeout)

# https://pypi.org/project/fake-useragent/
# ua = UserAgent()  # From here we generate a random user agent

# headers = {}
# headers['User-Agent'] = ua.chrome  # 'test:myappname:v0.0 (by /u/ocelost)'

DBC = "dbname='ip2location' user='postgres' host='localhost' password='1122' port='5432'"

# =======================================================================


class GoogleSearchImages:
    """
    This Scraper scrapes hotel names and hotel ids from hotelfriend.com website, \
    than makes a list of queries (hotel names) to be able to make a Google search request. \
    Spyder searches through socks5 proxies. Collects hotel images by categories, sources, \
    formats and stores them to PostgreSQL database.
    """
    def __init__(self, *args, **kwargs):
        self.gecko_path = os.path.abspath(os.path.curdir) + '/geckodriver'
        self.driver = webdriver.Firefox(executable_path=self.gecko_path)
        self.driver.implicitly_wait(15)
        self.conn = psycopg2.connect(DBC)
        self.cur = self.conn.cursor()
        self.conn.autocommit = True
        super(GoogleSearchImages, self).__init__(*args, **kwargs)

    def create_table_hotelfriend_deals(self):
        """ Create table hotelfriend deals """
        with self.conn:
            self.cur = self.conn.cursor()
            self.cur.execute("DROP TABLE IF EXISTS hotelfriend_deals")
            self.cur.execute(
                "CREATE TABLE hotelfriend_deals( \
                    unid TEXT, \
                    hotel_name TEXT, \
                    hotel_location TEXT, \
                    hotel_url_query TEXT, \
                    main TEXT, \
                    rooms TEXT, \
                    amenities TEXT, \
                    from_visitors TEXT, \
                    from_property TEXT, \
                    circle_view TEXT, \
                    videos TEXT, \
                    status BOOLEAN NOT NULL DEFAULT FALSE, \
                    used_queries TEXT);")
            print('hotelfriend_deals table created')

    def add_row(self):
        """ Add new row to table """
        with self.conn:
            self.cur = self.conn.cursor()
            self.cur.execute("ALTER TABLE hotelfriend_deals \
                                ADD COLUMN amenities TEXT, \
                                ADD COLUMN main TEXT;")
            print('hotelfriend_deals added new rows')

    def write_deal_ids_names_locations_to_postgres(self):
        """ Write deal ids, names, locations to Postgres """
        deal_ids = []
        deal_names = []
        deal_locations = []
        for i in range(0, 32):
            url = 'https://hotelfriend.com/deals/{}?type=dealPage'.format(i)
            response = urlopen(url).read().decode("utf8")
            tree = html.fromstring(response)

            deal_id = tree.xpath('//div/a[@class="hf-card_box"]/@href') or None
            ids = []
            for id in deal_id:
                did = str(id).replace("/a/id-", "").split("/(").pop(0)
                ids.append(did.strip())

            deal_name = tree.xpath('//p[@class="hotel_name"]/text()') or None
            hnames = []
            for n in deal_name:
                name = n.replace("\n        ", "").replace("\n      ", "")
                hnames.append(name.strip())

            deal_location = tree.xpath('//p[@class="hotel_location"]/text()') or None
            locations = []
            for l in deal_location[1::2]:
                location = l.replace("\n        ", "").replace("\n      ", "")
                locations.append(location.strip())

            deal_ids.append(ids)
            deal_names.append(hnames)
            deal_locations.append(locations)

        hotel_ids = []
        for deal_id in deal_ids:
            for id in deal_id:
                hotel_ids.append(id)

        hotel_names = []
        for deal_name in deal_names:
            for name in deal_name:
                hotel_names.append(name.replace("  ", "").strip())

        hotel_locations = []
        for deal_location in deal_locations:
            for location in deal_location:
                hotel_locations.append(location)

        for id, name, location in zip(hotel_ids, hotel_names, hotel_locations):
            print("Inserting : {} ++ {} ++ {}".format(id, name, location))
            # self.cur.execute("UPDATE hotelfriend_deals SET hotel_id = '%s';".format(id))
            self.cur.execute("INSERT INTO hotelfriend_deals (unid, hotel_name, hotel_location) \
                              VALUES(%s, %s, %s)", (id, name, location))

    def deleting_duplicates(self):
        """ Delete duplicates form DB """
        self.cur.execute("DELETE FROM hotelfriend_deals \
                            WHERE ctid NOT IN \
                            (SELECT MAX(dt.ctid) \
                                FROM hotelfriend_deals As dt \
                                GROUP BY dt.unid);")

    def select_hotel_names_without_id(self):
        """ Select hotel names without id """
        select_all_ids_without_id = "SELECT unid, hotel_name \
                                     FROM hotelfriend_deals \
                                     WHERE unid \
                                     LIKE '/a/%';"
        self.cur.execute(select_all_ids_without_id)
        db_hotel_ids_without_id = self.cur.fetchall()
        return db_hotel_ids_without_id

    def update_hotel_unids(self):
        """ Update hotel unids """
        self.cur.execute("UPDATE hotelfriend_deals \
                          SET unid = 'f4e5839753f31d8ce3526b98e404bb2c8a0afede' \
                          WHERE hotel_name = 'Wellness-Sport-Hotel Bayerischer Hof';")
        self.cur.execute("UPDATE hotelfriend_deals \
                          SET unid = 'cd351b627310b6b468fb20db1b4cfb7c76b56d6b' \
                          WHERE hotel_name = 'Jagdhotel Christopherhof';")
        self.cur.execute("UPDATE hotelfriend_deals \
                          SET unid = '08304fb9eca53c39febf948a2b777d9ef2a06301' \
                          WHERE hotel_name = 'Hotel am Steinbachtal';")
        self.cur.execute("UPDATE hotelfriend_deals \
                          SET unid = '289c1633f27d2ca8705e001f7c3f02522c071097' \
                          WHERE hotel_name = 'Kurhotel Quellenhof';")

    def update_hotel_status_by_select(self):
        """ Update hotel status by select """
        select_all_ids = "SELECT hotel_name, status \
                          FROM hotelfriend_deals \
                          WHERE status = 't';"
        self.cur.execute(select_all_ids)
        ids = self.cur.fetchall()
        print(ids)
        for id in ids:
            unid = id[0]
            update = "UPDATE hotelfriend_deals \
                      SET status = %s \
                      WHERE unid = %s;"
            try:
                with self.conn:
                    self.cur = self.conn.cursor()
                    self.cur.execute(update, ('f', unid))
                    print("Updatind next name: {}".format(unid))
            except Exception as err:
                print("Cannot insert: {}".format(err))

    def select_hotel_names(self):
        """ Select hotel names """
        select_all_ids_names = "SELECT unid, hotel_name, hotel_location \
                                FROM hotelfriend_deals;"
        self.cur.execute(select_all_ids_names)
        db_hotel_ids_names = self.cur.fetchall()
        return db_hotel_ids_names

    def making_google_query(self, hotel_ids_names):
        """ Making google query """
        for unid, hotel_name, hotel_location in hotel_ids_names:
            query = hotel_name + ' ' + hotel_location
            q = query.strip().replace("\xc8", "%C8").replace("\xe0", "%E0").replace("\xdc", "%DC").replace(
                "\xf3", "%F3").replace("\xdf", "%DF").replace("\xd6", "%D6").replace("\xe8", "%E8").replace(
                "\u2122", "%u2122").replace("\xf4", "%F4").replace("\xc4", "%C4").replace("\xe2", "%E2").replace(
                "\xe4", "%E4").replace("\xfc", "%FC").replace("\xe9", "%E9").replace("\xf6", "%F6").replace(" ", "%20").replace(
                "&", "%26").replace("'", "%27").replace("*", "%2A").replace("|", "%7C").replace("\'n", "%5C%27n").replace(
                "\'", "%5C%27").replace("/", "%2F").replace(" ", "%2B")

            create_url_query = "UPDATE hotelfriend_deals \
                                SET hotel_url_query = %s \
                                WHERE unid = %s;"
            self.cur.execute(create_url_query, (q, unid))

    def making_socks_proxy_request(self):
        """ Making socks proxy request """
        opener = urllib.request.build_opener(SocksiPyHandler(socks.SOCKS5, "188.25.85.234", 60209))
        urllib.request.install_opener(opener)
        url = 'http://httpbin.org/ip'
        response = urllib.request.urlopen(url).read().decode("utf8")
        print(response)

    def select_hotel_ids_and_queries(self):
        """ Select hotel ids and queries """
        select_all_ids_and_queries = "SELECT unid, hotel_url_query \
                                      FROM hotelfriend_deals \
                                      WHERE status = 'f';"
        self.cur.execute(select_all_ids_and_queries)
        db_hotel_ids_queries = self.cur.fetchall()
        return db_hotel_ids_queries

    def select_hotel_used_queries(self):
        """ Select hotel used queries """
        select_all_used_queries = "SELECT used_queries \
                                   FROM hotelfriend_deals;"
        self.cur.execute(select_all_used_queries)
        used_queries = self.cur.fetchall()
        return used_queries

    def making_images_urls(self, db_hotel_ids_queries, used_queries):
        """ Making images urls, start search, fing images, load to PostgreSQL """
        for unid, hotel_query in db_hotel_ids_queries:
            if hotel_query in used_queries:
                print()
                print("+++ Pass this one |<{}>| query is in the used queries list".format(hotel_query))
                continue

            try:
                url = 'http://httpbin.org/ip'
                response = urllib.request.urlopen(url).read().decode("utf8")
                print(response)
                self.driver.get("https://www.google.com/search?hl=en&q={}".format(hotel_query))
                print()
                print("Getting to hotel_query: {}".format(hotel_query))
                images_link = self.driver.find_element_by_xpath(
                    '//div[@class="luibli kno-fb-ctx"]/div/div[@class="thumb"]/a').get_attribute("href")
                print()
                print("following the images url: {}".format(images_link))
                self.driver.get("{}".format(images_link))
                image_types = self.driver.find_elements_by_class_name('gallery-tab-button')
                print()
                print("Images by types: {}".format([t.text for t in image_types]))

                for itype in image_types:
                    itype.click()
                    print("Scrapying images by next type: {}".format(itype.text))
                    if itype.text == 'ALL':
                        image_tag = self.driver.find_element_by_class_name('gallery-cell')
                        image_tag.click()
                        for i in range(0, 1000):
                            image_tag.send_keys(Keys.SPACE)
                        all_images_obj = self.driver.find_elements_by_xpath(
                            '//div[@id="main"]/jsl/div/a/div[@class="gallery-image-low-res"]/div[@class="gallery-image-high-res loaded"]')
                        all_image_list = [obj.value_of_css_property('background-image') for obj in all_images_obj]
                        all_imgs = [img.replace('url("', '').replace('")', '') for img in all_image_list if img != 'none']
                        print("ALL: {}".format(all_imgs))
                        self.cur.execute("UPDATE hotelfriend_deals \
                                          SET main = %s \
                                          WHERE unid = %s;", (all_imgs, unid))
                    elif itype.text == 'VIDEOS':
                        print("Current itype: {}".format(itype.text))
                        itype.click()
                        image_tag = self.driver.find_element_by_class_name('gallery-cell')
                        for i in range(0, 50):
                            image_tag.send_keys(Keys.SPACE)
                        current_url = (self.driver.current_url).split("&activetab=")
                        url_id = current_url[-1]
                        print("URL_ID: {}".format(url_id))
                        self.driver.switch_to.frame(self.driver.find_element_by_class_name('widget-scene-imagery-iframe'))
                        print(self.driver.page_source)
                        all_video_obj = self.driver.find_elements_by_tag_name('video')
                        video_src_list = [obj.get_attribute("src") for obj in all_video_obj]
                        video_poster_list = [obj.get_attribute("poster") for obj in all_video_obj]
                        videos = []
                        for src, poster in zip(video_src_list, video_poster_list):
                            video = src + ',' + poster
                            videos.append(video)
                        self.driver.switch_to.default_content()
                        print("Videos: {}".format(videos))
                        self.cur.execute("UPDATE hotelfriend_deals \
                                          SET videos = %s \
                                          WHERE unid = %s;", (videos, unid))
                    elif not itype.text == 'ALL':
                        print("Current itype: {}".format(itype.text))
                        itype.click()
                        image_tag = self.driver.find_element_by_class_name('gallery-cell')
                        for i in range(0, 200):
                            image_tag.send_keys(Keys.SPACE)
                        current_url = (self.driver.current_url).split("&activetab=")
                        url_id = current_url[-1]
                        print("URL_ID: {}".format(url_id))
                        images_obj = self.driver.find_elements_by_xpath(
                            '//div[@id="{}"]/jsl/div/a/div[@class="gallery-image-low-res"]/div[@class="gallery-image-high-res loaded"]'.format(url_id))
                        image_list = [obj.value_of_css_property('background-image') for obj in images_obj]
                        imgs = [img.replace('url("', '').replace('")', '') for img in image_list if img != 'none']
                        print(imgs)
                        set_type = itype.text.lower()
                        if set_type == 'rooms':
                            self.cur.execute("UPDATE hotelfriend_deals \
                                              SET rooms = %s \
                                              WHERE unid = %s;", (imgs, unid))
                        elif set_type == 'amenities':
                            self.cur.execute("UPDATE hotelfriend_deals \
                                              SET amenities = %s \
                                              WHERE unid = %s;", (imgs, unid))
                        elif set_type == 'from visitors':
                            self.cur.execute("UPDATE hotelfriend_deals \
                                              SET from_visitors = %s \
                                              WHERE unid = %s;", (imgs, unid))
                        elif set_type == 'from property':
                            self.cur.execute("UPDATE hotelfriend_deals \
                                              SET from_property = %s \
                                              WHERE unid = %s;", (imgs, unid))
                        elif set_type == '360° view':
                            self.cur.execute("UPDATE hotelfriend_deals \
                                              SET circle_view = %s \
                                              WHERE unid = %s;", (imgs, unid))

                    add_to_used_queries = "UPDATE hotelfriend_deals \
                                           SET used_queries = %s \
                                           WHERE unid = %s;"
                    print()
                    print("add_to_used_queries next query: {}".format(hotel_query))
                    self.cur.execute(add_to_used_queries, (hotel_query, unid))
                    update_status = "UPDATE hotelfriend_deals \
                                     SET status = %s \
                                     WHERE unid = %s;"
                    print("changing status to true of hotel_id: {}".format(unid))
                    self.cur.execute(update_status, ('t', unid))

            except NoSuchElementException as err:
                print("{}".format(err))
                self.driver.refresh()
                print("Updating page : {}".format(hotel_query))
                self.driver.refresh()


if __name__ == "__main__":
    google = GoogleSearchImages()
    # print("create_table_hotelfriend_deals")
    # google.create_table_hotelfriend_deals()
    # google.add_row()
    # print("write_deal_ids_names_locations_to_postgres")
    # google.write_deal_ids_names_locations_to_postgres()
    # print("deleting_duplicates")
    # google.deleting_duplicates()
    # db_hotel_ids_without_id = google.select_hotel_names_without_id()
    # print(db_hotel_ids_without_id)
    # google.update_hotel_unids()
    # print("updating unids without ids")
    # hotel_ids_names = google.select_hotel_names()
    # print(hotel_ids_names)
    # google.making_google_query(hotel_ids_names)
    google.making_socks_proxy_request()
    db_hotel_ids_queries = google.select_hotel_ids_and_queries()
    print(db_hotel_ids_queries)
    used_queries = google.select_hotel_used_queries()
    google.making_images_urls(db_hotel_ids_queries, used_queries)
    # google.update_hotel_status_by_select()
