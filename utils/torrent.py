from dataclasses import dataclass
import json
import os
from posix import unlink
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
import libtorrent as lt
import sys
from pathlib import Path
import re

from torrent_byteoffset_dl import (
    calculate_tar_end_offset,
    decode_tar_header,
    download_read_piece,
    find_tar_header,
    find_zip_header,
    decode_zip_header,
    calculate_zip_end_offset,
)
from utils.helpers import infohash_from_magnet

@dataclass
class ByteoffsetDownload:
    handle: Any
    byteoffset: int
    start_pieces: List[int]
    start_pieces_complete: bool
    filename: str


class FileNotFoundException(Exception):
    pass

class FailedToGetMetadataException(Exception):
    pass

class TorrentDownloader:
    def __init__(self, downloads_dir="./downloads") -> None:
        self.downloads_dir = downloads_dir

        self.session = lt.session({'dht_bootstrap_nodes': 'dht.libtorrent.org:25401,router.bittorrent.com:6881,dht.transmissionbt.com:6881,router.bt.ouinet.work:6881'})
        self.session.listen_on(6881, 6891)

        self.byteoffset_downloads: Dict[lt.torrent_handle, List[ByteoffsetDownload]]
        self.byteoffset_downloads = {}

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

    def _source_to_torrent_params(self, torrent_source: str) -> Tuple[Any, Literal['torrent', 'magnet', 'resume']]:
        params = {
            'save_path': self.downloads_dir,
            'storage_mode': lt.storage_mode_t.storage_mode_sparse,
        }
        source = None

        # If torrent_source is a magnet URI, set as 'url'
        if torrent_source.startswith("magnet:"):
            # try to read resume data
            resume = self._get_resume_data(
                infohash_from_magnet(torrent_source)
            )
            if resume:
                print("got resume data")
                return (resume, 'resume')

            params['url'] = torrent_source
            source = 'magnet'
        else:
            # assume .torrent path
            ti = lt.torrent_info(str(torrent_source))

            # try to read resume data
            resume = self._get_resume_data(
                str(ti.info_hash())
            )
            if resume:
                resume.ti = ti
                print("got resume data")
                return (resume, 'resume')

            params['ti'] = ti
            source = 'torrent'

        return (params, source)

    def _get_torrent_file_index_by_name(self, ti, filename: str):
        file_index = None
        for idx, f in enumerate(ti.files()):
            if Path(f.path).name == filename:
                file_index = idx
                break

        return file_index


    def _set_priorities(self, handle, filename: Union[str, List[str]], byteoffsets: List[int] = []):
        files_to_download = [filename] if type(filename) is str else filename

        ti = handle.get_torrent_info()
        num_files = ti.num_files()

        # if no files or parts spercified, download all
        if not len(files_to_download) and not len(byteoffsets):
            print("Downloading all files")
            priorities = [4] * num_files
            handle.prioritize_files(priorities)
            return

        # process byteoffsets
        if len(byteoffsets):
            self._byteoffset_set_pieces_priority(handle, byteoffsets)
            return

        files_indices = []

        print("Looking for files...", len(files_to_download))
        ti_files = ti.files()
        ti_files_map = {os.path.basename(f.path): index for index, f in enumerate(ti_files)}

        for file in files_to_download:
            file_index = ti_files_map.get(file)

            # TODO: add soft mode ???

            if file_index is None:
                # If need to download one single file
                if len(files_to_download) == 1 and not len(byteoffsets):
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

    def _byteoffset_set_pieces_priority(self, handle, byteoffsets: List[int]):
        info = handle.get_torrent_info()
        piece_size = info.piece_length()

        ti_files = info.files()

        # Disable all pieces first
        for p in range(info.num_pieces()):
            handle.piece_priority(p, 0)

        byteoffset_to_files = {}

        for start_offset in byteoffsets:
            # Convert absolute offset to piece index
            first_piece = start_offset // piece_size

            file_index = ti_files.file_index_at_piece(first_piece)
            file_path = ti_files.file_path(file_index)
            file_offset = ti_files.file_offset(file_index)

            byteoffset_to_files[start_offset] = {
                'path': file_path,
                'start_offset': file_offset,
            }

            # Download first 2 pieces now
            handle.piece_priority(first_piece, 7)
            handle.piece_priority(first_piece + 1, 7)

            bd = ByteoffsetDownload(
                handle=handle,
                byteoffset=start_offset,
                start_pieces=[first_piece, first_piece + 1],
                start_pieces_complete=False,
                filename = os.path.basename(file_path),
            )

            if handle not in self.byteoffset_downloads:
                self.byteoffset_downloads[handle] = []

            self.byteoffset_downloads[handle].append(bd)
            print("Added", bd, byteoffset_to_files)

            infohash = str(info.info_hash())
            with open(f"{self.downloads_dir}/{infohash}_byteoffsets.json", 'w') as f:
                json.dump(byteoffset_to_files, f)

    def is_bd_start_pieces_complete(self, handle):
        bd_list = self.byteoffset_downloads.get(handle, [])
        for dwn in bd_list:
            if not dwn.start_pieces_complete:
                return False

        return True

    def check_byteoffset_downloads(self):
        for bd_list in self.byteoffset_downloads.values():
            for dwn in bd_list:
                if dwn.start_pieces_complete:
                    continue

                have_start_pieces = True
                for piece in dwn.start_pieces:
                    if not dwn.handle.have_piece(piece):
                        have_start_pieces = False
                        break

                if not have_start_pieces:
                    print("Start pieces not downloaded yet", dwn.start_pieces)
                    s = dwn.handle.status()
                    print(f"[Start pieces] Progress: {s.progress * 100:.2f}%  down: {s.download_rate/1024:.1f} kB/s")
                    continue

                dwn.start_pieces_complete = True

                handle = dwn.handle
                info = handle.get_torrent_info()
                piece_size = info.piece_length()

                first_piece = dwn.start_pieces[0]
                piece_start = first_piece * piece_size

                data = download_read_piece(self.session, handle, first_piece)
                data += download_read_piece(self.session, handle, first_piece + 1)

                if dwn.filename.endswith('.zip'):
                    # looking for zip header
                    pt = find_zip_header(data, piece_start, dwn.byteoffset)
                    if pt is None:
                        print("Failed to find ZIP header")
                        continue
                    else:
                        print("Found ZIP header at", piece_start + pt)

                    header = decode_zip_header(data, pt)
                    print('Compressed size:', header.comp_size)

                    end_offset = calculate_zip_end_offset(header, piece_start, pt)
                    last_piece = end_offset // piece_size
                elif dwn.filename.endswith('.tar'):
                    pt = find_tar_header(data, piece_start, dwn.byteoffset)
                    if pt is None:
                        print("Failed to find TAR header")
                        continue
                    else:
                        print("Found TAR header at", piece_start + pt)

                    header = decode_tar_header(data, pt)
                    end_offset = calculate_tar_end_offset(header, piece_start, pt)
                else:
                    print("Unknown container", dwn.filename)
                    continue

                last_piece = end_offset // piece_size

                # Download all pieces
                for p in range(first_piece, last_piece + 1):
                    handle.piece_priority(p, 7)   # high priority


    def _wait_unitl_downloaded(self, handle, filename: Union[str, List[str]], progress_callback=None):
        # Progress loop (stops when file completed)
        ti = handle.get_torrent_info()
        if type(filename) is str:
            filenames = [filename]
        else:
            filenames = filename
        
        file_indexes = [self._get_torrent_file_index_by_name(ti, filename) for filename in filenames]
        

        def is_done(file_progress):
            done = False
            for idx in file_indexes:
                if idx is None:
                    # File is not in torrent
                    continue
                
                if file_progress[idx] >= ti.files().at(idx).size:
                    done = True
                elif done:
                    return False

            return done
        
        while True:
            s = handle.status()
            file_progress = handle.file_progress()  # bytes per file
            done = is_done(file_progress)
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
            if Path(f.path).name == filename:
                return f.path

        return None

    def pause_torrent(self, handle):
        handle.pause()

    def resume_torrent(self, handle):
        handle.resume()

    def force_recheck_torrent(self, handle):
        handle.force_recheck()

    def add(self, torrent_source: str, files: List[str], byteoffsets: List[int] = []):
        params, source = self._source_to_torrent_params(torrent_source)

        handle = self.session.add_torrent(params)
        print("Added torrent — waiting for metadata (if magnet)...")

        # Wait for metadata if magnet
        if source == 'magnet' or source == 'resume':
            count = 0
            while not handle.has_metadata():
                time.sleep(0.5)
                count +=1
                if count > 120:
                    self.session.remove_torrent(handle)
                    raise FailedToGetMetadataException(f"Failed to get metadata in {count / 2} secs")


        self._set_priorities(handle, files, byteoffsets)

        return handle

    def save_torrent_file(self, handle):
        torinfo = handle.torrent_file()  # Get the torrent info object
        torrent = lt.create_torrent(torinfo)  # Create a torrent file from the metadata

        # Bencode the torrent data (convert to the .torrent file format)
        torrent_data = lt.bencode(torrent.generate())

        ti = handle.get_torrent_info()
        infohash = str(ti.info_hash())

        # Save the .torrent file
        with open(f"{self.downloads_dir}/{infohash}.torrent", "wb") as f:
            f.write(torrent_data)

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

    def download(self, torrent_source: str, filename: Union[str, List[str]], progress_callback=None):
        params, source = self._source_to_torrent_params(torrent_source)

        handle = self.session.add_torrent(params)
        print("Added torrent — waiting for metadata (if magnet)...")

        # Wait for metadata if magnet
        if source == 'magnet' or source == 'resume':
            while not handle.has_metadata():
                time.sleep(0.5)

        self._set_priorities(handle, filename)
        self._wait_unitl_downloaded(handle, filename, progress_callback)

        return handle
        # self.session.remove_torrent(handle, delete_files=False)


if __name__ == "__main__":
    # Usage: python pick_file.py <torrent-or-magnet> <filename>
    if len(sys.argv) < 3:
        print("usage: python3 -m utils.torrent <torrent-file-or-magnet> <filename>")
        sys.exit(1)

    downloader = TorrentDownloader()
    downloader.download(sys.argv[1], sys.argv[2])
