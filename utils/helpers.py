import re

def infohash_from_magnet(magnet):
    match = re.search(r"btih:([a-fA-F0-9]{40}|[a-zA-Z0-9]{32})", magnet)
    if match:
        info_hash = match.group(1)
        return info_hash

    raise Exception("failed to extract infohash from magnet link")
