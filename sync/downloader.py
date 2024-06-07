from requests import get
from os import remove
from os.path import exists
from pathlib import Path
from time import sleep
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional, Tuple, List


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
        self.max_concurrency = max_concurrency or self.MAX_CONCURRENCY
        self.chunk = chunk or self.CHUNK
        self.progress = progress
        self.progress_args = progress_args
        self.content_length = self.__get_content_length()
        
    def __get_content_length(self) -> int:
        """
        Fetches the content length of the file from the server.

        Returns:
        ``int``: 
                The content length of the file.
        """
        r = get(self.url, stream=True)
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
    
    def __run_progress(self, chunked: int) -> None:
        """
        Updates the progress of the download and calls the progress callback if provided.

        Parameters:
        chunked (``int``): 
                The size of the chunk just downloaded.
        """
        
        self.current += chunked
        if not self.progress_args:
            self.progress_args = ()
        self.progress(self.current, self.content_length, chunked, *self.progress_args)

    def __downloader(self, client: int, range: Tuple[int, float]) -> None:
        """
        Downloads a specific range of the file in a separate thread.

        Parameters:
        client (``int``): 
                The client (thread) number.
        range (``tuple``): 
                The byte range to download.
        """
        r = get(self.url, stream=True, headers={'Range': f'bytes={int(range[0])}-{int(range[1] - 1)}'})
        
        with open(self.path.joinpath(f'{str(client)}_{self.file_name}.temp'), 'wb') as f:
            for chunk in r.iter_content(self.chunk):
                f.write(chunk)
                
                if self.progress:
                    self.__run_progress(len(chunk))
    
    def __merge_files(self) -> None:
        """
        Merges all the downloaded chunk files into the final file.
        """
        with open(self.path.joinpath(self.file_name), 'wb') as f1:
            for cl in self.__cl_counter():
                with open(cl, 'rb') as f2:
                    while True:
                        content = f2.read(1024 * 1024)
                        if not content:
                            break
                        f1.write(content)
    
    def __delete_temps(self) -> None:
        """
        Deletes all the temporary chunk files.
        """
        for cl in self.__cl_counter():
            if exists(cl):
                remove(cl)
    
    def download(self) -> None:
        """
        Initiates the download process using multiple threads.
        """
        with ThreadPoolExecutor(self.max_concurrency) as pool:
            tasks = [pool.submit(self.__downloader, range[0], range[1]) for range in self.__ranges()]
            for task in tasks:
                try:
                    task.result()
                except:
                    sleep(1)
                    self.__delete_temps()
                    raise ConnectionRefusedError("Connection refused, temp files will be deleted.")
            
            self.__merge_files()
            self.__delete_temps()

# Example usage with progress bar using tqdm
from tqdm import tqdm

if __name__ == "__main__":
    bar = tqdm(
        total=int(get('https://dl2.soft98.ir/soft/n/Notepad.8.6.8.x86.rar?1717760731').headers['Content-Length']),
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    )
    
    def progress_callback(current: int, total: int, chunked: int, bar: tqdm) -> None:
        bar.update(chunked)
    
    downloader = FileDownloader(
        'https://dl2.soft98.ir/soft/n/Notepad.8.6.8.x86.rar?1717760731',
        'c:/Users/pc/Desktop',
        'a.rar',
        progress=progress_callback,
        progress_args=(bar,)
    )
 
    downloader.download()

