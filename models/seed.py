from typing import Optional
from dataclasses import dataclass

@dataclass
class SeedModel:
	file_id: int
	filename: str
	magnet_link: str
	ipfs_cid: Optional[str] = None

	seed_id: Optional[int] = None
