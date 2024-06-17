import asyncio
import aiohttp
from aiofiles import os
from aiofile import async_open
from pathlib import Path
from typing import Callable, Optional, Tuple, List
from aiohttp_socks import ProxyConnector

class FileDownloader:
    # Class variables for default maximum concurrency and chunk size
    MAX_CONCURRENCY = 4
    CHUNK = 1024
    current = 0
    
    def __init__(
        self,
        url: str,
        path: str,
        file_name: str,
        proxy:str = '',
        auth:dict = None,
        chunk: int = None,
        max_concurrency: int = None,
        progress: Optional[Callable[[int, int, int, Tuple], None]] = None,
        progress_args: Optional[Tuple] = None
    ) -> None:
        """
        Initializes the DownloadFile instance with the provided parameters.

        Parameters:
        url (``str``): 
                The URL of the file to download.
        path (``str``): 
                The local directory path to save the downloaded file.
        file_name (``str``): 
                The name of the file to save as.
        chunk (``int``, *optional*): 
                The size of each chunk to download. Default is 1024 bytes.
        max_concurrency (``int``, *optional*): 
                Maximum number of concurrent download threads. Default is 4.
        progress (``Callable``, *optional*):
                Pass a callback function to view the file transmission progress.
                The function must take *(current, total, chunked)* as positional arguments (look at Other Parameters below for a
                detailed description) and will be called back each time a new file chunk has been successfully
                transmitted.

        progress_args (``tuple``, *optional*):
                Extra custom arguments for the progress callback function.
                You can pass anything you need to be available in the progress callback scope.
        """
        if not path:
            raise ValueError('Path not exists')
        path = Path(path)
        if not path.exists():
            raise ValueError('Path not exists')
        if not path.is_dir():
            raise ValueError('Path must be a directory')
        
        self.url = url
        self.path = path
        self.file_name = file_name
        self.proxy = proxy
        self.auth = auth
        self.max_concurrency = max_concurrency or self.MAX_CONCURRENCY
        self.chunk = chunk or self.CHUNK
        self.progress = progress
        self.progress_args = progress_args
        

    async def __get_content_length(self, session) -> int:
        """
        Fetches the content length of the file from the server.

        Returns:
        ``int``: 
                The content length of the file.
        """
        
        r = await session.get('https://api.myip.com')

        r = await session.get(self.url)
        return int(r.headers.get('Content-Length'))
        
    def __ranges(self) -> List[Tuple[int, Tuple[int, float]]]:
        """
        Splits the content length into ranges for concurrent downloading.

        Returns:
        ``list``: 
                A list of tuples containing range information.
        """
        content_length = self.content_length
        ranges = []
        current = 0
        c = 1
        while current < content_length:
            ranges.append((c, (current, content_length / self.max_concurrency * c)))
            current += content_length / self.max_concurrency
            c += 1
        return ranges
    
    def __cl_counter(self) -> List[Path]:
        """
        Generates a list of temporary file paths for the download chunks.

        Returns:
        ``list``: 
                A list of temporary file paths.
        """
        return [self.path.joinpath(f'{i}_{self.file_name}.temp') for i in range(1, self.max_concurrency + 1)]
    
    async def __run_progress(self, chunked: int) -> None:
        """
        Updates the progress of the download and calls the progress callback if provided.

        Parameters:
        chunked (``int``): 
                The size of the chunk just downloaded.
        """
        
        self.current += chunked
        if not self.progress_args:
            self.progress_args = ()
        await self.progress(self.current, self.content_length, chunked, *self.progress_args)

    async def __downloader(self, session, client: int, range: Tuple[int, float]) -> None:
        """
        Downloads a specific range of the file in a separate thread.

        Parameters:
        client (``int``): 
                The client (thread) number.
        range (``tuple``): 
                The byte range to download.
        """
        
        r = await session.get(self.url,
            headers={'Range': f'bytes={int(range[0])}-{int(range[1] - 1)}'},
            auth=self.auth)
    
        async with async_open(self.path.joinpath(f'{str(client)}_{self.file_name}.temp'), 'wb') as f:
            async for chunk in r.content.iter_chunked(self.chunk):
                await f.write(chunk)
        
                if self.progress:
                    await self.__run_progress(len(chunk))
    
    async def __merge_files(self) -> None:
        """
        Merges all the downloaded chunk files into the final file.
        """
        async with async_open(saved:=self.path.joinpath(self.file_name), 'wb') as f1:
            for cl in self.__cl_counter():
                async with async_open(cl, 'rb') as f2:
                    while True:
                        content = await f2.read(1024 * 1024)
                        if not content:
                            break
                        await f1.write(content)
        return saved
    
    async def __delete_temps(self) -> None:
        """
        Deletes all the temporary chunk files.
        """
        for cl in self.__cl_counter():
            if await os.path.exists(cl):
                await os.remove(cl)
    
    async def download(self) -> None:
        """
        Initiates the download process using multiple threads.
        """
        connector = ProxyConnector.from_url(self.proxy)
        async with aiohttp.ClientSession(auth=self.auth, connector=connector) as session:
            self.content_length = await self.__get_content_length(session)
            tasks = [self.__downloader(session, range[0], range[1]) for range in self.__ranges()]
            try:
                await asyncio.gather(*tasks)
            except Exception as e:
                print(e)
                await asyncio.sleep(1)
                await self.__delete_temps()
                raise ConnectionRefusedError("Connection refused, temp files will be deleted.")
            
            path = await self.__merge_files()
            await self.__delete_temps()
            
        return path
    
    


# r = httpx.Client(proxies={'http://':'socks5://127.0.0.1:10808',
#                           'https://': 'socks5://127.0.0.1:10808'})



# Example usage with progress bar using tqdm
# from tqdm import tqdm, asyncio
import aiohttp.connector
from tqdm.asyncio import tqdm
import requests
if __name__ == "__main__":
    # bar = tqdm(
    #     total=int(requests.get('https://dl2.soft98.ir/soft/c/Codec.Tweak.Tool.6.7.2.rar?1718381181').headers['Content-Length']),
    #     unit='iB',
    #     unit_scale=True,
    #     unit_divisor=1024,
    # )
    
    # async def progress_callback(current: int, total: int, chunked: int, bar: tqdm) -> None:
    #     bar.update(chunked)
    
    downloader = FileDownloader(
        'https://dl2.soft98.ir/soft/c/Codec.Tweak.Tool.6.7.2.rar',
        'c:/Users/pc/Desktop',
        'a.rar',
        # proxy='socks5://127.0.0.1:10808'
               
        # progress=progress_callback,   
        # progress_args=(bar,)
    )
 
    asyncio.run(downloader.download())

