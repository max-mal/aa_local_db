from typing import List, Optional
from dataclasses import dataclass, field
import zlib

@dataclass
class FileModel:

	title: Optional[str]
	extension: str
	year: Optional[int]

	md5: str

	# ; separated list of file paths
	server_path: str

	file_id: Optional[int] = None

	# ; separated list of ipfs cids
	ipfs_cid: Optional[str] = None

	torrent: Optional[str] = None
	torrent_id: Optional[int] = None
	torrent_magnet_link: Optional[str] = None

	description: Optional[str] = None
	description_compressed: Optional[bytes] = None

	cover_url: Optional[str] = None
	author: Optional[str] = None
	languages: List[str] = field(default_factory=list)

	# File offset in torrent file (if file is packed in zip/tar archive)
	byteoffset: Optional[int] = None

	is_journal: bool = False

	is_complete: Optional[bool] = None
	local_path: Optional[str] = None


	def set_description_compressed(self):
		if not self.description:
			return

		if not self.description_compressed:
			self.description_compressed = zlib.compress(self.description.encode("utf-8"))

	def load_description(self, compressed):
		if not compressed:
			return

		self.description = zlib.decompress(compressed).decode("utf-8")
