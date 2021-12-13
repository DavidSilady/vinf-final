import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause
from org.apache.lucene.document import Document, Field, StringField, TextField, IntPoint
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.queryparser.classic import QueryParser
import simplemma
import csv

lang_data = simplemma.load_data('sk')

# search test
links_index_path = "./indexing/indexes/links"
links_directory = SimpleFSDirectory(Paths.get(links_index_path))
links_searcher = IndexSearcher(DirectoryReader.open(links_directory))
analyzer = StandardAnalyzer()

sentences_index_path = "./indexing/indexes/sentences"
sentences_directory = SimpleFSDirectory(Paths.get(sentences_index_path))
sentences_searcher = IndexSearcher(DirectoryReader.open(sentences_directory))


def search_examples(word, num=2):
    query = QueryParser("content", analyzer).parse(word)
    score_docs = sentences_searcher.search(query, num).scoreDocs

    examples = []
    for score_doc in score_docs:
        doc = sentences_searcher.doc(score_doc.doc)
        examples.append(doc)
    return examples


def search(word, distance=0, num=5):
    query = BooleanQuery.Builder()
    form_parser = QueryParser("form", analyzer).parse(word + "~" + str(distance))
    lemma_parser = QueryParser("lemma", analyzer).parse(word + "~" + str(distance))
    query.add(form_parser, BooleanClause.Occur.SHOULD)
    query.add(lemma_parser, BooleanClause.Occur.SHOULD)
    built_query = query.build()
    score_docs = links_searcher.search(built_query, num).scoreDocs
    print("=======================================================")
    print("Word:", word)
    print("Distance:", distance)

    for i, score_doc in enumerate(score_docs):
        print("Result", i)
        doc = links_searcher.doc(score_doc.doc)
        print('lemma:', doc.get("lemma"))
        print('postfix:', doc.get("postfix"))
        print('content:', doc.get("content"))

    if len(score_docs) == 0:
        print("Result")
        print("simplemma:", simplemma.lemmatize(word, lang_data))

    examples = search_examples(word)
    if len(examples) != 0:
        print("examples:")
    for example in examples:
        print(example)


