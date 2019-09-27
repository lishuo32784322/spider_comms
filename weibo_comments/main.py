def main():
    from scrapy import cmdline
    cmdline.execute(['scrapy', 'crawl', 'comments','-s','JOBDIR=job_info/comments'])

# if __name__ == '__main__':
#     print('weibo_comments_spider')
main()