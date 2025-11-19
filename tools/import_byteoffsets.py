import sys
import json

from repositories.files import FilesRepository
from utils.db import connect_db


class ImportByteoffsetsTool:

    BATCH_SIZE = 1000

    def __init__(self):
        self.db = connect_db()
        self.repo = FilesRepository(self.db, self.db.cursor())


    def run(self):
        count = 0
        wr_count = 0
        self.db.execute("BEGIN")  # start transaction

        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)

                metadata = doc.get('metadata', {})
                md5 = metadata.get('md5')
                byteoffset = metadata.get('byte_start')

                if not md5:
                    continue

                wr_count += self.repo.set_byteoffset_by_md5(md5, byteoffset)

                count += 1
                # commit every BATCH_SIZE
                if count % self.BATCH_SIZE == 0:
                    print(count, wr_count)

                if wr_count and (wr_count % self.BATCH_SIZE == 0):
                    print('commit')
                    wr_count = 0
                    self.db.commit()
                    self.db.execute("BEGIN")  # start new transaction

            except Exception as e:
                sys.stderr.write(f"Error processing line: {e}\n")
                raise e

        self.db.commit()  # commit remaining records
        self.db.close()


if __name__ == '__main__':
    ImportByteoffsetsTool().run()
