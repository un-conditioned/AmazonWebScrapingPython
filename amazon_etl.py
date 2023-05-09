import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np

URL = "https://www.amazon.in/gp/bestsellers/computers/1375424031"
HEADERS = ({'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36','Accept-Language':'en-US,en;q=0.5'})

 # Function to extract Product Title
def get_title(page):

    try:
        # Outer Tag Object
        title = page.find("span", attrs={"id":'productTitle'})
        
        # Inner NavigatableString Object
        title_value = title.text

        # Title as a string value
        title_string = title_value.strip()

    except AttributeError:
        title_string = ""

    return title_string

# Function to extract Product Price
def get_price(page):

    try:
        price = page.find("span", attrs={"class": "a-offscreen"}).text

    except AttributeError:

        try:
            # If there is some deal price
            price = page.find("span", attrs={'id':'priceblock_dealprice'}).string.strip()

        except:
            price = ""

    return price
# Function to extract Product Rating
def get_rating(page):

    try:
        rating = page.find("span",attrs={"class":'a-icon-alt'}).get_text(strip=True)
    
    except AttributeError:
        try:
            rating = page.find("span", attrs={'class':'a-icon-alt'}).string.strip()
        except:
            rating = ""	

    return rating

# Function to extract Number of User Reviews
def get_review_count(page):
    try:
        review_count = page.find("span", attrs={'id':'acrCustomerReviewText'}).text

    except AttributeError:
        review_count = ""	

    return review_count

# Function to extract Availability Status
def get_availability(page):
    try:
        available = page.find("div", attrs={'id':'availability'})
        available = available.find("span").string.strip()

    except AttributeError:
        available = "Not Available"	

    return available

def uploadtos3(d):
    df = pd.DataFrame.from_dict(d)
    df['title'].replace('', np.nan, inplace=True)
    df = df.dropna(subset=['title'])
    timestr = timestr = time.strftime("%Y%m%d-%H%M%S")
    df.to_csv("s3://laptop-data-bucket/INPUT_DATA/amazon_data_"+timestr+".csv", header=True, index=False)



def run_amazon_etl():
    webpage = requests.get(URL, headers=HEADERS)
    type(webpage.content)

    #Soup object containing all data
    soup = BeautifulSoup(webpage.content, "html.parser")

    # Fetch links as List of Tag Objects
    links = soup.find_all("a", attrs={'class':'a-link-normal'})
    #Loop for extracting links from Tag objects and then product details from each link
    product_links_list =[]
    dict = {"title":[], "price":[], "rating":[], "reviews":[],"availability":[]}
    for link in links:
        product_links_list.append(link.get('href'))
        
    for product_link in product_links_list:
        product_link = "https://www.amazon.in" + product_link 
        print(product_link)
        new_webpage = requests.get(product_link, headers=HEADERS)
        new_soup = BeautifulSoup(new_webpage.content, "html.parser")
        dict['title'].append(get_title(new_soup))
        dict['price'].append(get_price(new_soup))
        dict['rating'].append(get_rating(new_soup))
        dict['reviews'].append(get_review_count(new_soup))
        dict['availability'].append(get_availability(new_soup))
    else:    
        uploadtos3(dict)
    



    