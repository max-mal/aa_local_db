from typing import List, Optional
from dataclasses import dataclass, field

@dataclass
class TorrentFileModel:
	torrent_id: int
	filename: str
	file_id: int

	is_complete: bool = False

	local_path: Optional[str] = None
	torrent_file_id: Optional[int] = None
	byteoffset: Optional[int] = None


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

	is_seeding: bool = False
	is_seed_all: bool = False

	files: List[TorrentFileModel] = field(default_factory=list)
