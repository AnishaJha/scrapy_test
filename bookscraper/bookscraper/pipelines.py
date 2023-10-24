# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import mysql.connector
import re
import scrapy
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class BookscraperPipeline:
    def process_item(self, item: scrapy.Item, spider):
        adapter = ItemAdapter(item)

        # Strip whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'decription':
                value = adapter.get(field_name)
                adapter[field_name] = value.strip()
        # Switch to lower case category and product_type
        lower_case_keys = ['category', 'product_type']
        for lower_case_key in lower_case_keys:
            value: str = adapter.get(lower_case_key)
            adapter[lower_case_key] = value.lower()
        # Convert £ to $ in price
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value: str = adapter.get(price_key)
            adapter[price_key] = value.replace('£', '$')

        # convert availability to numeric
        value: str = adapter.get('availability')
        new_value = re.search("\d+", value)
        if not new_value:
            availability = 0
        else:
            availability = int(new_value.group())
        adapter['availability'] = availability

        # convert star rating to integer
        value: str = adapter.get('stars').split(" ")[1].lower()
        new_value = 0
        if value == "one":
            new_value = 1
        elif value == "two":
            new_value = 2
        elif value == "three":
            new_value = 3
        elif value == "four":
            new_value = 4
        elif value == "five":
            new_value = 5

        adapter['stars'] = new_value

        return item


class SaveToMySQLPipeline:

    def __init__(self):
        self.conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='books'
        )

        # Create cursor, used to run commands
        self.cur = self.conn.cursor()
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS books(
        id int NOT NULL auto_increment,
        url VARCHAR(255),
        title text,
        product_type VARCHAR(255),
        price_excl_tax VARCHAR(255),
        price_incl_tax VARCHAR(255),
        tax VARCHAR(255),
        price VARCHAR(255),
        availability INTEGER,
        stars INTEGER,
        category VARCHAR(255),
        DESCRIPTION text,
        PRIMARY KEY (id)
        )
        """)

    def process_item(self, item: scrapy.Item, spider):
        self.cur.execute("""insert into books(
        url,
        title,
        product_type,
        price_excl_tax,
        price_incl_tax,
        tax,
        price,
        availability,
        stars,
        category,
        DESCRIPTION
        ) values (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
        )
        """, (
            item["url"],
            item["title"],
            item["product_type"],
            item["price_excl_tax"],
            item["price_incl_tax"],
            item["tax"],
            item["price"],
            item["availability"],
            item["stars"],
            item["category"],
            str(item["description"][0])
        ))
        self.conn.commit()
        return item

    def close_spider(self, spider):
        # close cursor and connection to database
        self.cur.close()
        self.conn.close()
