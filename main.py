import aiohttp
from aiohttp import ClientSession
import asyncio
from lxml import etree
from costum_queue import MESSAGE_QUEUE
from urllib.parse import urljoin
from typing import Dict
import re
import aiofiles


# http://guba.eastmoney.com/list,zssh000001,f_7.html
# 按发帖时间进行抓取20000页


async def request_list_page(session: ClientSession, url: str) -> None:
    async with session.get(url) as resp:
        html = await resp.text(encoding="utf8")
        element = etree.HTML(html)
        articles = element.xpath("//div[contains(@class, 'articleh')]")
        for article in articles:
            try:
                item = dict()
                item['title'] = article.xpath("./span[3]/a/text()")[0]
                item['author'] = article.xpath('./span[4]/a/font/text()')[0]
                item['url'] = urljoin(url, article.xpath('./span[3]/a/@href')[0])
                await request_detail_page(session, item)
            except Exception as e:
                print(e)


async def request_detail_page(session: ClientSession, msg: Dict) -> None:
    async with session.get(msg['url']) as resp:
        html = await resp.text(encoding="utf8")
        res = re.search('''<div class="zwfbtime">发表于\s(.*?:\d\d)\s.*?</div>''', html, re.S)
        # 如果是广告页面则提取不到
        if res:
            msg['time'] = res.group(1)
            MESSAGE_QUEUE.append(msg)


async def save_to_csv() -> None:
    async with aiofiles.open('data.csv', mode="a", encoding="utf8") as f:
        for msg in MESSAGE_QUEUE:
            await f.write(f"{msg['title']},{msg['author']},{msg['time']}\n")


async def main():
    async with aiohttp.TCPConnector(
            limit=200,
            force_close=True,
            enable_cleanup_closed=True,
    ) as tc:
        async with aiohttp.ClientSession(connector=tc) as session:
            i = 1
            while i < 20000:
                task = (asyncio.ensure_future(request_list_page(
                    session=session,
                    url=f"http://guba.eastmoney.com/list,zssh000001,f_{i}.html"
                )) for i in range(i, i + 20))
                i += 20
                await asyncio.gather(*task)
                await save_to_csv()
                print(i)


if __name__ == '__main__':
    asyncio.run(main())
