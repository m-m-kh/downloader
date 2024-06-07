from requests import get
from os import remove
from os.path import exists
from pathlib import Path
from time import sleep
from concurrent.futures import ThreadPoolExecutor


class DownloadFile:
    MAX_CONCURRENCY = 4

    def __init__(self, url, path, file_name, max_concurrency:int=MAX_CONCURRENCY) -> None:
        if not path:
            raise ValueError('Path not exists')    
        path = Path(path)
        if not path.exists():
            raise ValueError('Path not exists')
        if not path.is_dir():
            raise ValueError('path must be directory')
        
        self.url = url
        self.path = path
        self.file_name = file_name
        self.max_concurrency = max_concurrency
        
        
    def __get_content_length(self):
        r = get(self.url, stream=True)
        return int(r.headers.get('Content-Length'))
        
    def __ranges(self):
        content_length = self.__get_content_length()
        ranges = []
        current = 0
        c = 1
        while current<content_length:
            ranges.append((c,(current,content_length/self.max_concurrency*c)))
            current+=content_length/self.max_concurrency
            c+=1
        return ranges
    
    def __cl_counter(self):
        return [self.path.joinpath(f'{i}_{self.file_name}.temp') for i in range(1,self.max_concurrency+1)]
        
    
    def downloader(self, client, range):
        r = get(self.url,
                    stream=True,
                    headers= {'Range': f'bytes={int(range[0])}-{int(range[1]-1)}'})
        print(range[0],range[1],r.headers['content-range'])
        
        with open(self.path.joinpath(f'{str(client)}_{self.file_name}.temp'),'wb') as f:
            for i in r.iter_content(1024):
                f.write(i) 
                     
        print('downladed')
    
    def __merge_files(self):
        with open(self.path.joinpath(self.file_name), 'wb') as f1:
            for cl in self.__cl_counter():
                print(cl)
                with open(cl, 'rb') as f2:
                    while True:
                        content = f2.read(1024*1024)
                        if not content:
                            break
                        f1.write(content)
                print('merged')  
                
    def __delete_temps(self):
        for cl in self.__cl_counter():
            if exists(cl):
                remove(cl)
    
    def download(self):
        
        with ThreadPoolExecutor(self.max_concurrency) as pool:
            tasks = [pool.submit(self.downloader,range[0], range[1]) for range in self.__ranges()]
        
            for i in tasks:
                try:    
                    i.result()
                except:
                    time.sleep(1)
                    self.__delete_temps()
                    raise ConnectionRefusedError("connection refused, temps files will be delete.")                    
                
            self.__merge_files()
            self.__delete_temps()
            
 
 
if __name__=="__main__":
    r = DownloadFile('https://dl2.soft98.ir/soft/s/ShutUp10.v1.9.1438.411.rar?1717756302', 'c:/Users/pc/Desktop', 'a.rar')
    import time
    s = time.time()
    print(r.download())
    e = time.time()

    print(e-s)
            
