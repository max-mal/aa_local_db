import time
from typing import List, Union
import libtorrent as lt
import sys
from pathlib import Path

class FileNotFoundException(Exception):
    pass

class FailedToGetMetadataExtension(Exception):
    pass

class TorrentDownloader:
    def __init__(self, downloads_dir="./downloads") -> None:
        self.downloads_dir = downloads_dir

        self.session = lt.session()
        self.session.listen_on(6881, 6891)

    def _source_to_torrent_params(self, torrent_source: str):
        params = {
            'save_path': self.downloads_dir,
        }

        # If torrent_source is a magnet URI, set as 'url'
        if torrent_source.startswith("magnet:"):
            params['url'] = torrent_source
        else:
            # assume .torrent path
            ti = lt.torrent_info(str(torrent_source))
            params['ti'] = ti

        return params

    def _get_torrent_file_index_by_name(self, ti, filename: str):
        file_index = None
        for idx, f in enumerate(ti.files()):
            if Path(f.path).name == filename or f.path.endswith(filename):
                file_index = idx
                break

        return file_index

    def _set_priorities(self, handle, filename: Union[str, List[str]]):
        ti = handle.get_torrent_info()

        # find the index for the desired file
        files_to_download = [filename] if type(filename) is str else filename
        files_indices = []

        for file in files_to_download:
            file_index = self._get_torrent_file_index_by_name(ti, file)
            if file_index is None:
                raise FileNotFoundException(
                    f"File '{filename}' not found in torrent. Available files:\n" +
                        "\n".join([str(Path(f.path)) for f in ti.files()])
                )

            files_indices.append(file_index)

        num_files = ti.num_files()

        # 0 = do not download, 7 = max priority (libtorrent uses 0-7)
        priorities = [0] * num_files
        for file_index in files_indices:
            priorities[file_index] = 7

        handle.prioritize_files(priorities)
        print(f"Downloading only file index {files_indices} -> {filename}")

    def _wait_unitl_downloaded(self, handle, filename: str, progress_callback=None):
        # Progress loop (stops when file completed)
        ti = handle.get_torrent_info()
        file_index = self._get_torrent_file_index_by_name(ti, filename)

        while True:
            s = handle.status()
            file_progress = handle.file_progress()  # bytes per file
            done = file_progress[file_index] >= ti.files().at(file_index).size
            print(f"overall: {s.progress*100:.2f}%, download rate {s.download_rate/1000:.1f} kB/s, file done: {done}")

            if progress_callback:
                progress_callback({
                    'progress': s.progress,
                    'download_rate': s.download_rate,
                    'done': done,
                })

            if done:
                print("File download complete.")
                break
            time.sleep(1)

    def torrent_status(self, handle):
        s = handle.status()
        return {
            'progress': s.progress,
            'download_rate': s.download_rate,
            'upload_rate': s.upload_rate,
        }

    def add(self, torrent_source: str, files: List[str]):
        params = self._source_to_torrent_params(torrent_source)

        handle = self.session.add_torrent(params)
        print("Added torrent — waiting for metadata (if magnet)...")

        # Wait for metadata if magnet
        count = 0
        while not handle.has_metadata():
            time.sleep(0.5)
            count +=1
            if count > 120:
                self.session.remove_torrent(handle)
                raise FailedToGetMetadataExtension(f"Failed to get metadata in {count / 2} secs")

        self._set_priorities(handle, files)

        return handle

    def save_resume_data(self, torrent_handle):
        if not torrent_handle.is_valid() or not torrent_handle.has_metadata:
            return False

        torrent_handle.save_resume_data()
        return True

    def download(self, torrent_source: str, filename: str, progress_callback=None):
        params = self._source_to_torrent_params(torrent_source)

        handle = self.session.add_torrent(params)
        print("Added torrent — waiting for metadata (if magnet)...")

        # Wait for metadata if magnet
        while not handle.has_metadata():
            time.sleep(0.5)

        self._set_priorities(handle, filename)
        self._wait_unitl_downloaded(handle, filename, progress_callback)
        # self.session.remove_torrent(handle, delete_files=False)


if __name__ == "__main__":
    # Usage: python pick_file.py <torrent-or-magnet> <filename>
    if len(sys.argv) < 3:
        print("usage: python pick_file.py <torrent-file-or-magnet> <filename>")
        sys.exit(1)

    downloader = TorrentDownloader()
    downloader.download(sys.argv[1], sys.argv[2])
