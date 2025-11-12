from typing import Optional
from dataclasses import dataclass

@dataclass
class TorrentModel:
	path: str

	torrent_id: Optional[int] = None
	magnet_link: Optional[str] = None
	added_to_torrents_list_at: Optional[str] = None

	data_size: Optional[int] = None
	num_files: Optional[int] = None

	obsolete: Optional[bool] = None
	embargo: Optional[bool] = None
