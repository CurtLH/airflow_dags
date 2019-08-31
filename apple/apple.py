from bs4 import BeautifulSoup as bs
from collections import defaultdict
import requests


def get_urls():

    r = requests.get("https://www.apple.com/shop/refurbished/mac/macbook-pro")
    soup = bs(r.content, "html.parser")
    ads = soup.find("div", {"class": "refurbished-category-grid-no-js"})
    urls = set()
    for a in ads.find_all("a", href=True):
        urls.add("https://www.apple.com" + a["href"].split("?")[-1])

    return urls


def get_html(url):

    r = requests.get(url)
    if r.status_code == 200:
        return r.text
    else:
        raise ValueError(r.status_code)


def get_specs(soup):

    specs = defaultdict(list)
    section = soup.find(
        "div", {"class": "as-productinfosection-panel TechSpecs-panel row"}
    )
    for cat in section.select(".h3-para-title"):
        k = cat.text.strip()
        for item in cat.find_next_siblings():
            if item.name != "div":
                break
            else:
                specs[k.lower()].append(item.text.strip().lower())

    return dict(specs)


def get_price(soup):

    price = soup.find(
        "div",
        {"class": "as-price-currentprice as-pdp-currentprice as-pdp-refurbishedprice"},
    )
    price = price.findAll("span")[1]
    price = price.getText().replace("\n", "").strip()
    price = price.replace("$", "").replace(",", "")
    price = float(price)

    return price


def get_date(soup):

    specs = soup.find(
        "div", {"class": "as-productinfosection-mainpanel column large-9 small-12"}
    )
    for tag in specs.findAll("p"):
        parsed = tag.getText()
        if "released" in parsed:
            date = parsed.replace("\n", "").strip().lower()
            break
        else:
            date = ""

    return date


def get_screen(soup):

    specs = soup.find(
        "div", {"class": "as-productinfosection-mainpanel column large-9 small-12"}
    )
    for tag in specs.findAll("p"):
        parsed = tag.getText()
        if "-inch" in parsed.lower() and not parsed.startswith("http"):
            screen = parsed.replace("\n", "").strip().lower()
            break
        else:
            screen = ""

    return screen.strip().lower()


def get_color(url):

    if "space-gray" in url.lower():
        return "space-gray"
    elif "silver" in url.lower():
        return "silver"
    else:
        return "N/A"


def get_id_num(url):

    return url.split("/")[5].lower()
