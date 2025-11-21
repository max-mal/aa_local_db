import cProfile
from tools.import_json import ImportJsonTool

tool = ImportJsonTool()
cProfile.run("tool.run('books')")
