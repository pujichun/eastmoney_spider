# 东方财富spider

抓取上证指数吧中的帖子标题、作者、发帖时间。

因为抓取的列表页的帖子涉及年份跨越问题，并且使用的是异步抓取策略，所以想要获取到详细时间还需要从详情页进一步提取

抓取完成之后写入到csv中，使用aiofile进行异步写入，aiofile更新之后API有所更改