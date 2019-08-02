import requests, bs4
from bs4 import BeautifulSoup

url = "https://quote-feed.zacks.com/index.php?t="
rank_set = {'1','2','3','4','5'}

def zacks_rank(stock):
    try:
        r=requests.get(url+stock)
        soup = bs4.BeautifulSoup(r.text,"html.parser")
        rank = soup.text[soup.text.find('"zacks_rank":"')+14]
        if rank in rank_set:
            return rank
        else:
            return 'NA'
    except:
        print("Problem to get Zack Rank for {}".format(stock))
        return 'NA'
