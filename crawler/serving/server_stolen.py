from flask import Flask, request, jsonify
import csv
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import ssl
from flask_cors import CORS
app = Flask(__name__)


CORS(app)
"""
# NLTK setup (handles SSL certificate issues)
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
"""

# Download NLTK data only if not already downloaded
def download_nltk_resources():
    try:
        stopwords.words('english')
    except LookupError:
        nltk.download('stopwords')
    try:
        word_tokenize('test')
    except LookupError:
        nltk.download('punkt')
        
# Initialize NLTK components
download_nltk_resources()
stop_words = set(stopwords.words('english'))
ps = PorterStemmer()



def load_inverted_index(filePath):
    invertedIndex = {}
    with open(filePath, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            word = row['word']
            docIDs2Str = row['docIDs'].strip("[]")  # Remove brackets
            docIDsList = docIDs2Str.split(', ') if docIDs2Str else []
            docIDs = set(int(docID) for docID in docIDsList)
            invertedIndex[word] = docIDs
    return invertedIndex



def load_document_info(filePath):
    documentInfo = {}
    with open(filePath, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            docID = int(row['docID'])
            documentInfo[docID] = {
                'url': row['url'],
                'title': row['title'],
                'description': row['description'],
                'pagerank': float(row['pagerank'])
            }
    return documentInfo



def parse_query(query):
    # Tokenize the query
    tokens = word_tokenize(query.lower())
    # Remove non-alphabetic tokens and stop words, then stem the words
    query_words = [
        ps.stem(word) for word in tokens if word.isalpha() and word not in stop_words
    ]
    return query_words



def search(query, inverted_index, document_info, num_results=10, page=1):
    query_words = parse_query(query)
    if not query_words:
        return []
    # Find documents that contain any of the query words
    matched_doc_ids = set()
    for word in query_words:
        if word in inverted_index:
            matched_doc_ids.update(inverted_index[word])
    if not matched_doc_ids:
        return []
    # Retrieve documents and their PageRank scores
    results = []
    for doc_id in matched_doc_ids:
        info = document_info[doc_id]
        results.append({
            'doc_id': doc_id,
            'url': info['url'],
            'title': info['title'],
            'description': info['description'],
            'pagerank': info['pagerank']
        })
    # Sort documents by PageRank score 
    sorted_results = sorted(results, key=lambda x: x['pagerank'], reverse=True)
    # Pagination
    start = (page - 1) * num_results
    end = start + num_results
    paginated_results = sorted_results[start:end]
    return paginated_results

# Load the inverted index and document info
# If you are using a different file, replace the path with the path to your file
#If you're using a database, replace this with the code to connect to your database
try:
    inverted_index = load_inverted_index('../search/complete_examples/advanced_pagerank_inverted_index.csv')
    document_info = load_document_info('../search/complete_examples/advanced_pagerank.csv')
except FileNotFoundError:
    try:
        inverted_index = load_inverted_index("../advanced_pagerank_inverted_index.csv")
        document_info = load_document_info("../advanced_pagerank.csv")
    except FileNotFoundError:
        print("Error: Files not found, run the advanced_pagerank.py file first")
        print("Exiting...")
        exit()
        

@app.route('/search')
def search_api():
    query = request.args.get('q', '')
    num_results = int(request.args.get('num_results', 10))
    page = int(request.args.get('page', 1))
    if not query:
        return jsonify({'error': 'No query provided'}), 400
    results = search(query, inverted_index, document_info, num_results=num_results, page=page)
    return jsonify({
        'query': query,
        'page': page,
        'num_results': num_results,
        'results': results
    })

if __name__ == '__main__':
    app.run(debug=True)