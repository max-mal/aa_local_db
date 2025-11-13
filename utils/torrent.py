import os
from posix import unlink
import time
from typing import List, Optional, Union
import libtorrent as lt
import sys
from pathlib import Path
import re

class FileNotFoundException(Exception):
    pass

class FailedToGetMetadataException(Exception):
    pass

class TorrentDownloader:
    def __init__(self, downloads_dir="./downloads") -> None:
        self.downloads_dir = downloads_dir

        self.session = lt.session()
        self.session.listen_on(6881, 6891)

    def infohash_from_magnet(self, magnet):
        match = re.search(r"btih:([a-fA-F0-9]{40}|[a-zA-Z0-9]{32})", magnet)
        if match:
            info_hash = match.group(1)
            return info_hash

        raise Exception("failed to extract infohash from magnet link")

    def _get_resume_data(self, infohash: str):
        resume_path = os.path.join(
            self.downloads_dir,
            infohash + '.fastresume'
        )

        try:
            if os.path.exists(resume_path):
                with open(resume_path, 'rb') as f:
                    return lt.read_resume_data(f.read())
        except Exception as e:
            print("Failed to read resume data:", e)
            return None

    def _source_to_torrent_params(self, torrent_source: str):
        params = {
            'save_path': self.downloads_dir,
        }

        # If torrent_source is a magnet URI, set as 'url'
        if torrent_source.startswith("magnet:"):
            # try to read resume data
            resume = self._get_resume_data(
                self.infohash_from_magnet(torrent_source)
            )
            if resume:
                print("got resume data")
                return resume

            params['url'] = torrent_source
        else:
            # assume .torrent path
            ti = lt.torrent_info(str(torrent_source))

            # try to read resume data
            resume = self._get_resume_data(
                str(ti.info_hash())
            )
            if resume:
                print("got resume data")
                return resume

            params['ti'] = ti

        return params

    def _get_torrent_file_index_by_name(self, ti_files, filename: str):
        file_index = None
        for idx, f in enumerate(ti.files()):
            if Path(f.path).name == filename or f.path.endswith(filename):
                file_index = idx
                break

        return file_index


    def _set_priorities(self, handle, filename: Union[str, List[str]]):
        files_to_download = [filename] if type(filename) is str else filename

        ti = handle.get_torrent_info()
        num_files = ti.num_files()

        if len(files_to_download) == 0:
            print("Downloading all files")
            priorities = [4] * num_files
            handle.prioritize_files(priorities)
            return

        files_indices = []

        print("Looking for files...", len(files_to_download))
        ti_files = ti.files()
        ti_files_map = {os.path.basename(f.path): index for index, f in enumerate(ti_files)}

        for file in files_to_download:
            file_index = ti_files_map.get(file)

            # TODO: add soft mode ???

            if file_index is None:
                if len(files_to_download) == 1:
                    self.remove_torrent(handle, delete_files=True)

                    raise FileNotFoundException(
                        f"File '{file}' not found in torrent. Available files:\n" +
                            "\n".join([str(Path(f.path)) for f in ti_files])
                    )
                else:
                    print(f"File '{file}' not found in torrent")
            else:
                files_indices.append(file_index)

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
        ti = handle.get_torrent_info()

        file_progress = handle.file_progress()  # bytes per file
        progress = {}

        for index, item in enumerate(file_progress):
            if item == 0:
                continue

            progress[index] = item / ti.files().at(index).size

        return {
            'progress': s.progress,
            'download_rate': s.download_rate,
            'upload_rate': s.upload_rate,
            'files': progress,
        }

    def torrent_files(self, handle):
        ti = handle.get_torrent_info()
        return [f.path for f in ti.files()]

    def get_torrent_file_path_by_name(self, handle, filename: str) -> Optional[str]:
        ti = handle.get_torrent_info()
        for f in ti.files():
            if Path(f.path).name == filename or f.path.endswith(filename):
                return f.path

        return None

    def pause_torrent(self, handle):
        handle.pause()

    def resume_torrent(self, handle):
        handle.resume()

    def force_recheck_torrent(self, handle):
        handle.force_recheck()

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
                raise FailedToGetMetadataException(f"Failed to get metadata in {count / 2} secs")

        self._set_priorities(handle, files)

        return handle

    def save_resume_data(self, torrent_handle):
        if not torrent_handle.is_valid() or not torrent_handle.has_metadata:
            return False

        torrent_handle.save_resume_data()
        return True

    def remove_resume_data(self, torrent_handle):
        ti = torrent_handle.get_torrent_info()
        infohash = str(ti.info_hash())
        resume_path = os.path.join(self.downloads_dir, infohash + '.fastresume')

        if os.path.exists(resume_path):
            unlink(resume_path)

    def process_alerts(self):
        alerts = self.session.pop_alerts()
        for a in alerts:
            if isinstance(a, lt.save_resume_data_alert):
                print(a)
                data = lt.write_resume_data_buf(a.params)
                h = a.handle
                ti = h.get_torrent_info()
                infohash = str(ti.info_hash())
                open(os.path.join(self.downloads_dir, infohash + '.fastresume'), 'wb').write(data)

            if isinstance(a, lt.save_resume_data_failed_alert):
                print('failed to save resume data')

    def remove_torrent(self, torrent_handle, delete_files=False):
        self.session.remove_torrent(torrent_handle, (1 if delete_files else 0))

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
