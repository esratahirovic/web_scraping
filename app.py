from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
import time
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from apscheduler.schedulers.background import BlockingScheduler
## kullanılacak olan paketlerin import edilmesi


class Bookstore:
    # database configurasyonlarının yapılması
    s = Service(executable_path="/home/esra/Desktop/chromedriver_linux64/chromedriver")
    driver = webdriver.Chrome(service=s)

    # mongodb bağlantısı
    client = MongoClient('mongodb://localhost:27017')
    db = client["bookstores"]
   
    # collectionların csv dosyasına dönüştürülmesi
    def collection_to_csv(self, collection, csv_file):
        cursor = collection.find({})
        df = pd.DataFrame(list(cursor))
        df.to_csv(csv_file, index=False)
    
    # kitapyurdu sitesi için tanımlanan fonksiyon
    def kitapyurdu(self):
        ky_col= self.db["kitapyurdu"] 
        start = time.time()
        self.driver.get("https://www.kitapyurdu.com/") 
        self.driver.maximize_window()
        time.sleep(3)

        searchbox = self.driver.find_element(By.ID, "search-input")
        searchbox.send_keys("python")
        searchbox.send_keys(Keys.ENTER)

        time.sleep(3)

        limit = Select(self.driver.find_element(By.XPATH, "/html/body/div[5]/div/div/div[3]/div[4]/div[1]/div/div[2]/select"))
        limit.select_by_visible_text("50 Ürün")
        time.sleep(1)

        items = self.driver.find_elements(By.XPATH, "//div[@id='product-table']//div[@class='product-cr']")
        count = 0

        for item in items:

            price_old_elements = item.find_elements(By.CLASS_NAME, "price-old")
            price_new_elements = item.find_elements(By.CLASS_NAME, "price-new")
                
            if price_old_elements and price_new_elements:
                price = price_new_elements[0].find_element(By.CLASS_NAME, "value").text.strip()
            elif price_new_elements:
                price = price_new_elements[0].find_element(By.CLASS_NAME, "value").text.strip()
            else:
                price = "unknown"

            time.sleep(2) 
   
            title = item.find_element(By.CLASS_NAME, "name").text.strip()
            print(title)
            publisher = item.find_element(By.CLASS_NAME, "product-details").find_element(By.CLASS_NAME, "publisher").text.strip()
            print(publisher)                
            writer = item.find_element(By.CLASS_NAME, "product-details").find_element(By.CLASS_NAME, "author").find_element(By.CLASS_NAME, "alt").text.strip()
            print(writer)                    
            print(price)
            count +=1
            print(f"{count}---------------------------------------------")                  
            

            if price == "unknown":
                        pass
            else:
                if ky_col.find_one({"title": title, "publisher": publisher, "writers": writer}) is None:
                        ky_col.insert_one({
                                                    "title": title,
                                                    "publisher": publisher,
                                                    "writers": writer,
                                                    "price": price
                                                })
        end = time.time()
        print(f"{end-start:.2f} saniyede işlem tamamlandı. \n")       

        self.collection_to_csv(ky_col, "kitapyurdu.csv")
                
    # kitapsepeti sitesi için tanımlanan fonksiyon
    def kitapsepeti(self):
        ks_col = self.db["kitapsepeti"]
        start = time.time()
        self.driver.get("https://www.kitapsepeti.com/")
        self.driver.maximize_window()
        time.sleep(2)

        searchbox = self.driver.find_element(By.ID, "live-search")
        searchbox.send_keys("python")
        searchbox.send_keys(Keys.ENTER)

        checkbox = self.driver.find_element(By.XPATH, "/html/body/div[2]/div/div/div/div/div/div/div/div/div[1]/div/div[3]/div[1]/div/div/div/div/label").click()
        time.sleep(2)

        items = self.driver.find_element(By.CLASS_NAME, "fl.col-12.catalogWrapper").find_elements(By.CLASS_NAME, "col.col-3.col-md-4.col-sm-6.col-xs-6.p-right.mb.productItem.zoom.ease")
        print(len(items))
        count = 0
        for item in items:

            title = item.find_element(By.CLASS_NAME, "box.col-12.text-center").find_element(By.CLASS_NAME, "fl.col-12.text-description.detailLink").text.strip()
            print(title)
            publisher = item.find_element(By.CLASS_NAME, "box.col-12.text-center").find_element(By.CLASS_NAME, "col.col-12.text-title.mt").text.strip()
            print(publisher)                
            writer = item.find_element(By.CLASS_NAME, "box.col-12.text-center").find_element(By.CLASS_NAME, "fl.col-12.text-title").text.strip()
            print(writer)
            price = item.find_element(By.CLASS_NAME, "col.col-12.currentPrice").text.strip()
            price = price.split()[0]                  
            print(price)                                           
            count +=1
            print(f"{count}---------------------------------------------") 

            if ks_col.find_one({"title": title, "publisher": publisher, "writers": writer}) is None:
                ks_col.insert_one({
                        "title": title,
                        "publisher": publisher,
                        "writers": writer,
                        "price": price
                    })
        end = time.time()
        print(f"{end-start:.2f} saniyede işlem tamamlandı. \n") 

        self.collection_to_csv(ks_col, "kitapsepeti.csv")

    
scheduler = BlockingScheduler()

# güncelleme fonksiyonu
def updates():
    app = Bookstore()
    app.kitapsepeti()
    app.kitapyurdu()
    update_time = datetime.now()
    update_time = update_time.strftime("%Y-%m-%d - %H:%M")
    print(f"Son güncelleme {update_time} tarihinde yapıldı.")
    

app = Bookstore()
app.kitapyurdu()
app.kitapsepeti()


## pandas operasyonları
ktpsepeti = pd.read_csv("kitapsepeti.csv", header=0)
ktpyurdu = pd.read_csv("kitapyurdu.csv", header=0)


print(ktpyurdu.head())
print(ktpsepeti.head())


# dataframe in uygun formata getirilmesi
def df_prepare(df):
    df.drop("_id", axis=1, inplace=True)
    df["price"] = df["price"].str.replace(",", ".").astype(float)
    df["publisher"] = df["publisher"].str.title()
    return df

# writer veya publisher columnlarından istenilen için group by işlemi yapılması
def groupby_func_writer_or_publisher(df,col):
    result = df.groupby(col).agg({"title": "count", "price": "mean"}).sort_values(by="title", ascending=False)
    print(result)

# farklı sitelerde bulunan aynı kitaba ait price karşılaştırılması
def compare_prices(row):
    if row["writers_x"] == row["writers_y"]:
        if row["price_x"] > row["price_y"]:
            print(f"{row['title']} : kitapsepeti sitesinde daha uygun fiyatlı.")
        elif row["price_x"] < row["price_y"]:
            print(f"{row['title']} : kitapyurdu sitesinde daha uygun fiyatlı.")
        else:
            print(f"{row['title']} : iki platformda da aynı fiyata satışta.")
    else:
        return ""

ktpyurdu = df_prepare(ktpyurdu)
ktpsepeti = df_prepare(ktpsepeti)

groupby_func_writer_or_publisher(ktpyurdu,"publisher")
groupby_func_writer_or_publisher(ktpsepeti,"writers")


df = ktpsepeti.merge(ktpyurdu, on="title", how="inner")

df.apply(compare_prices, axis=1)


scheduler.add_job(updates, "cron", day_of_week='0-6', hour="12", minute="0", start_date=datetime.now())

scheduler.start()
