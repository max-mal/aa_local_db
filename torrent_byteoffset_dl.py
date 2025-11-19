from collections import namedtuple
import libtorrent as lt
import time
import os
import struct
import zlib


ZIP_HEADER_SIZE = 30
ZipHeader = namedtuple(
    'ZipHeader', (
        'version_needed',
        'flags',
        'comp_method',
        'mod_time',
        'mod_date',
        'crc32',
        'comp_size',
        'uncomp_size',
        'fname_len',
        'extra_len'
    )
)

TAR_HEADER_SIZE = 512
TarHeader = namedtuple(
    'TarHeader', (
        'filename',
        'filesize'
    )
)


def download_read_piece(session, h, number: int):
    while not h.have_piece(number):
        s = h.status()
        print(f"Progress: {s.progress * 100:.2f}%  down: {s.download_rate/1024:.1f} kB/s")
        time.sleep(1)

    h.read_piece(number)
    done = False
    while not done:
        alerts = session.pop_alerts()
        for a in alerts:
            if isinstance(a, lt.read_piece_alert):
                if a.piece == number:
                    return a.buffer

    raise Exception("Failed to read piece")



def find_zip_header(data: bytes, piece_start: int, start_offset: int):
    start_in_piece = start_offset - piece_start
    pt = start_in_piece
    while pt >= 0:
        if data[pt:pt+4] == b'PK\x03\x04':
            return pt
        pt -=1

    return None


def decode_zip_header(data: bytes, header_start: int):
    # header is 26 bytes, b'PK\x03\x04' preceeding
    header = data[header_start + 4:header_start + ZIP_HEADER_SIZE]

    unpacked = struct.unpack("<HHHHHIIIHH", header)
    return ZipHeader(*unpacked)


# header - ZipHeader structure
# first_piece_start - first_piece start byteoffset
# pt - location of zip header in first piece
def calculate_zip_end_offset(header: ZipHeader, first_piece_start: int, pt: int):
    return first_piece_start + pt + ZIP_HEADER_SIZE + \
        header.fname_len + header.extra_len + header.comp_size


def get_zip_compressed_data(data: bytes, header: ZipHeader, pt: int):
    comp_start = pt + ZIP_HEADER_SIZE + \
        header.fname_len + header.extra_len

    return data[comp_start:comp_start + header.comp_size]


def get_zip_filename(data: bytes, header: ZipHeader, pt: int):
    fname_start = pt + ZIP_HEADER_SIZE
    filename = data[fname_start:fname_start + header.fname_len].decode()
    if not filename:
        return None

    return filename

def decompress_zip_data(compressed_data: bytes, header: ZipHeader):
    # Decompress if needed
    if header.comp_method == 0:      # Stored
        return compressed_data
    elif header.comp_method == 8:    # Deflated
        return zlib.decompress(compressed_data, -zlib.MAX_WBITS)
    else:
        raise NotImplementedError(f"Compression method {header.comp_method} not supported")


def find_tar_header(data: bytes, piece_start: int, start_offset: int):
    start_in_piece = start_offset - piece_start
    pt = start_in_piece
    while pt >= 0:
        if data[pt + 257:pt+262] == b'ustar':
            return pt
        pt -=1

    return None


def decode_tar_header(data: bytes, header_start: int):
    # header is 512 bytes
    header = data[header_start:header_start + TAR_HEADER_SIZE]

    filename = header[0:100].rstrip(b'\x00').decode('utf-8')

    # Parse file size (octal string)
    size_str = header[124:136].rstrip(b'\x00').decode('utf-8')
    file_size = int(size_str, 8)  # Convert from octal

    return TarHeader(filename, file_size)


def get_tar_compressed_data(data: bytes, header: TarHeader, pt: int):
    comp_start = pt + TAR_HEADER_SIZE

    return data[comp_start:comp_start + header.filesize]


# header - ZipHeader structure
# first_piece_start - first_piece start byteoffset
# pt - location of zip header in first piece
def calculate_tar_end_offset(header: TarHeader, first_piece_start: int, pt: int):
    return first_piece_start + pt + TAR_HEADER_SIZE + \
        header.filesize


def start_torrent(session, torrent_file, save_path: str):
    info = lt.torrent_info(torrent_file)
    h = session.add_torrent({
        "ti": info,
        "save_path": save_path,
        "storage_mode": lt.storage_mode_t.storage_mode_sparse,
        "flags": lt.torrent_flags.sequential_download
    })

    return h

def download_byte_range(session, handle, start_offset: int):
    h = handle
    info = h.get_torrent_info()
    ti_files = info.files()

    piece_size = info.piece_length()

    # Convert absolute offset to piece index
    first_piece = start_offset // piece_size
    piece_start = first_piece * piece_size  # 0

    # Disable all pieces first
    for p in range(info.num_pieces()):
        h.piece_priority(p, 0)

    need_download_next_piece = True if start_offset - piece_start < 512 else False

    # Download first 2 pieces now
    h.piece_priority(first_piece, 7)

    if need_download_next_piece:
        h.piece_priority(first_piece + 1, 7)

    # Wait for first pieces to download
    print("Downloading first pieces...")
    data = bytes()
    data += download_read_piece(session, h, first_piece)
    if need_download_next_piece:
        data += download_read_piece(session, h, first_piece + 1)

    file_index = ti_files.file_index_at_piece(first_piece)
    file_path: str
    file_path = ti_files.file_path(file_index)

    if file_path.endswith('.zip'):
        # looking for zip header
        pt = find_zip_header(data, piece_start, start_offset)
        if pt is None:
            raise Exception("Failed to find ZIP header")
        else:
            print("Found ZIP header at", piece_start + pt)

        header = decode_zip_header(data, pt)
        print('Compressed size:', header.comp_size)

        end_offset = calculate_zip_end_offset(header, piece_start, pt)
    elif file_path.endswith('.tar'):
        pt = find_tar_header(data, piece_start, start_offset)
        if pt is None:
            raise Exception("Failed to find TAR header")
        else:
            print("Found TAR header at", piece_start + pt)

        header = decode_tar_header(data, pt)
        end_offset = calculate_tar_end_offset(header, piece_start, pt)
    else:
        raise Exception("Unknown container")

    last_piece = end_offset // piece_size

    # Download all pieces
    for p in range(first_piece, last_piece + 1):
        h.piece_priority(p, 7)   # high priority

    buffer = bytes()
    for p in range(first_piece, last_piece + 1):
        buffer += download_read_piece(session, h, p)

    print("All pieces downloaded")

    if file_path.endswith('.zip'):
        # Extracting data and filenames
        assert(type(header) is ZipHeader)
        comp_data = get_zip_compressed_data(buffer, header, pt)
        filename = get_zip_filename(buffer, header, pt)
        if filename is None:
            raise Exception("Failed to read filename")

        return (filename, decompress_zip_data(comp_data, header))
    elif file_path.endswith('.tar'):
        assert(type(header) is TarHeader)
        comp_data = get_tar_compressed_data(buffer, header, pt)
        return (header.filename, comp_data)
    else:
        raise Exception("Unknown container")


if __name__ == "__main__":
    import sys

    torrent_file = sys.argv[1]
    download_path = "./downloads"
    start_byte = sys.argv[2]

    session = lt.session()
    handle = start_torrent(session, torrent_file, download_path)

    filename, data = download_byte_range(session, handle, int(start_byte))

    with open(os.path.basename(filename), 'wb') as f:
        f.write(data)

    print("Extracted", filename)
