import lucene
from org.apache.lucene.store import FSDirectory
from java.nio.file import Paths
from org.apache.lucene.index import DirectoryReader
from java.nio.file import Paths
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.search import TermQuery
from org.apache.lucene.index import Term
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.queryparser.classic import QueryParser
import pandas as pd

# Initialize the Lucene VM
lucene.initVM()

# Path to the Lucene index directory
index_path = "smalldocsindex/"

#Open the index directory
index_dir = FSDirectory.open(Paths.get(index_path))
# create reader object
reader = DirectoryReader.open(index_dir)

# instatiate/define reader
searcher = IndexSearcher(reader)

analyzer = StandardAnalyzer()
query_parser = QueryParser("text_content", analyzer)  # Using the same analyzer as indexing

#EVALUATION

df_dev_query_results = pd.read_csv("smalldataset/dev_query_results_small.csv")
df_dev_queries = pd.read_excel("smalldataset/dev_small_queries.xlsx")
df_merged_results = df_dev_query_results.merge(df_dev_queries,left_on="Query_number",right_on="Query number",)
df_merged_results = df_merged_results.rename(columns={"doc_number":"DocumentName"})

unique_queries = df_merged_results['Query'].unique()

def mean_average_precision_at_k(top_k_docs, relevant_docs):
    precision_at_k = []
    num_relevant = 0
    top_k_docs = top_k_docs.set_index("DocumentName")
    for i, doc in enumerate(top_k_docs.index, start=1):
        if doc in relevant_docs:
            num_relevant += 1
            precision_at_k.append(num_relevant / i)

    if precision_at_k:
        return sum(precision_at_k) / len(relevant_docs)
    else:
        return 0.0

def mean_average_recall_at_k(top_k_docs, relevant_docs):
    top_k_docs = top_k_docs.set_index("DocumentName")

    num_relevant_retrieved = len(set(top_k_docs.index) & set(relevant_docs))
    num_relevant_total = len(relevant_docs)

    if num_relevant_total > 0:
        return num_relevant_retrieved / num_relevant_total
    else:
        return 0.0


k = 3
results = []
counter = 0
test_results_df_top3 = pd.DataFrame(columns=["Query_number", "doc_number"])


for query in unique_queries:

    counter += 1
    try:
        preprocessed_query = query_parser.parse(query)  # Parse the user query
        hits = searcher.search(preprocessed_query, k).scoreDocs  # Get top 10 results
        print("df_merged_results: \n", df_merged_results[df_merged_results['Query'] == query])
        relevant_docs = df_merged_results[df_merged_results['Query'] == query]['DocumentName'].astype(str).tolist()
        query_number = str(df_merged_results[df_merged_results['Query'] == query]["Query_number"].values[0])
        top_k_docs = pd.DataFrame(columns=['DocumentName', 'Score'])
    except Exception as e:
        print("problem with query:", query)
    for hit in hits:
        doc = searcher.storedFields().document(hit.doc)
        # print('path:', doc.get("path"), 'name:', doc.get("path"))
        #print(f"Found document with content: {doc.get('text_content')}")
        #print(f"Path: {doc.get('path')}")
        #print(f"Score: {hit.score}")
        new_records = pd.DataFrame({
            'Query_number': [query_number],  # Repeat 155 for each new row
            'doc_number': doc.get('path').split(".txt")[0].split("_")[-1] # Values for B
        })
        test_results_df_top3 = pd.concat([test_results_df_top3, new_records], ignore_index=True)
        new_records = pd.DataFrame({
            'DocumentName': [doc.get('path').split(".txt")[0].split("_")[-1]],  # Repeat 155 for each new row
            'Score': [hit.score]  # Values for B
        })
        top_k_docs = pd.concat([top_k_docs, new_records], ignore_index=True)
        #print("INFOO")
        #print(top_k_docs["DocumentName"].values)
        #print(relevant_docs)

    map_k = mean_average_precision_at_k(top_k_docs, relevant_docs)
    mar_k = mean_average_recall_at_k(top_k_docs, relevant_docs)
    results.append({'Query': query, 'MAP@K': map_k, 'MAR@K': mar_k})

sum_mar3 = 0
for i in results:
    sum_mar3 += i["MAR@K"]
mar3 = sum_mar3 / len(results)

sum_map3 = 0
for i in results:
    sum_map3 += i["MAP@K"]
map3 = sum_map3 / len(results)

print("SCORE:")
print("MAR@3:", mar3)
print("MAP@3:", map3)
print(results)
reader.close()
