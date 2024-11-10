import lucene

# print(lucene.VERSION)

from java.nio.file import Paths
from org.apache.lucene.store import FSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriterConfig
from org.apache.lucene.index import IndexWriter, IndexOptions
from org.apache.lucene.document import Document, TextField, Field, FieldType
import os

print(lucene.VERSION)

# intitialize VM to adapt java lucene to python
lucene.initVM()

index_dir = FSDirectory.open(Paths.get("largedocsindex"))

analyzer = StandardAnalyzer()

indexWriterConfig = IndexWriterConfig(analyzer)

indexWriter = IndexWriter(index_dir, indexWriterConfig)

# Index bulk files
t1 = FieldType()
t1.setStored(True)
t1.setTokenized(False)
t1.setIndexOptions(IndexOptions.DOCS_AND_FREQS)

def index_txt_file(ind_writer, file):
    doc = Document()
    f = open(file, "r")
    text_to_index = f.read()
    # print(text_to_index)
    doc.add(Field("path", file.split(".txt")[0].split("_")[-1], t1))
    doc.add(TextField("text_content", text_to_index, Field.Store.YES))
    ind_writer.addDocument(doc)

# Path to the directory (absolute or relative)
data_dir = "largedataset/full_docs"
# os.listdir return a list of all files within
# the specified directory
for file in os.listdir(data_dir):

    # The following condition checks whether
    # the filename ends with .txt or not
    if file.endswith(".txt"):
        # Appending the filename to the path to obtain
        # the fullpath of the file
        data_path = os.path.join(data_dir, file)
        print(data_path)
        index_txt_file(indexWriter, data_path)

indexWriter.close()

print("Indexing complete.")

