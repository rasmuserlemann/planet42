import pandas as pd
from icrawler.builtin import GoogleImageCrawler
import os
import base64

from icrawler import ImageDownloader
from icrawler.builtin import GoogleImageCrawler
from six.moves.urllib.parse import urlparse
from icrawler.utils import Signal

from datetime import datetime
 

ExcelFileName= 'Mosaic.xlsx'

df = pd.read_excel(ExcelFileName)
df['YoM'] = df['YoM'].fillna(0).astype('int')
x = df.to_string(header=False,
                  index=False,
                  index_names=False).split('\n')

car_names = [' '.join(row.split()) for row in x]
print(car_names)


class MyImageDownloader(ImageDownloader):
    
    def get_filename(self, task, default_ext):
        url_path = urlparse(task['file_url'])[2]
        now = datetime.now()
        dt_string = now.strftime("%d_%m_%Y_%H_%M_%S")
        print(os.path.basename(url_path))

        return os.path.basename(url_path)

Ncars = len(car_names)

filters = dict(
    size = 'medium',
    #license='commercial,modify' #change this to control if commercial images are allowed or not
    )


package = 0
for i in range(1000*package,(package+1)*1000): #range(Ncars):
#    print(i)
    #i=234
    #print(i)
    print(i)
    print(car_names[i])

    google_crawler = GoogleImageCrawler(
        downloader_cls=MyImageDownloader,
        log_level=100,
        storage={'root_dir': 'TEST_package' + str(package)})
    
    
    google_crawler.crawl(car_names[i],filters = filters, max_num=2)

