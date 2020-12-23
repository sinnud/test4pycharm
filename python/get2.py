import requests
from lxml import etree
from fake_useragent import UserAgent
class qidian(object):
    def __init__(self, url, target_dir, name):
        #self.url = 'https://book.qidian.com/info/1020580616#Catalog'
        self.url=url
        self.target_dir=target_dir
        self.name=name
        ua = UserAgent(verify_ssl=False)
        for i in range(1, 100):
            self.headers = {
                'User-Agent': ua.random
            }
    def get_html(self,url):
        response=requests.get(url,headers=self.headers)
        html=response.content.decode('utf-8')
        return html
    def parse_html(self,html):
        target=etree.HTML(html)
        links=target.xpath('//ul[@class="cf"]/li/a/@href')
        with open(self.target_dir + '/' + self.name + '.txt', 'a') as f:
            for link in links:
                host='https:'+link
                #解析链接地址
                res=requests.get(host,headers=self.headers)
                c=res.content.decode('utf-8')
                target=etree.HTML(c)
                names=target.xpath('//span[@class="content-wrap"]/text()')
                results=target.xpath('//div[@class="read-content j_readContent"]/p/text()')
                for name in names:
                    print(name)
                    f.write(f"\n\n{name}\n\n")
                    for result in results:
                        f.write(result+'\n')
    def main(self):
        url=self.url
        html=self.get_html(url)
        self.parse_html(html)
if __name__ == '__main__':
    spider=qidian('https://book.qidian.com/info/1020580616#Catalog'
    , '/var/local/data/novel'
    , "大周仙吏"
    )
    spider.main()