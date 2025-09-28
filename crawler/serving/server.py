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



def parse_query(query):
    stopWords = set(stopwords.words('english'))
    ps = PorterStemmer()
    # Tokenize the query
    tokens = word_tokenize(query.lower())
    # Remove non-alphabetic tokens and stop words, then stem the words
    queryWords = [
        ps.stem(word) for word in tokens if word.isalpha() and word not in stopWords
    ]
    return queryWords



def load_invertedIndex(filePath):
    invertedIndex = {}
    with open(filePath, 'r', encoding='utf-8') as csvFile:
        reader = csv.DictReader(csvFile)
        for row in reader:
            word = row['word']
            docIDsStr = row['docIDs'].strip("[]")  # Remove brackets
            docIDsList = docIDsStr.split(', ') if docIDsStr else []
            docIDs = set(int(docID) for docID in docIDsList)
            invertedIndex[word] = docIDs
    return invertedIndex



def load_documentInfo(filePath):
    documentInfo = {}
    with open(filePath, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            docID = int(row['docID'])
            documentInfo[docID] = {
                'url': row['url'],
                'title': row['title'],
                'description': row['description'],
                'pageRank': float(row['pageRank'])
            }
    return documentInfo



def search(query, invertedIndex, documentInfo, numResults=10, page=1):
    queryWords = parse_query(query)
    if not queryWords:
        return []
    # Find documents that contain any of the query words
    matchedDocIDs = set()
    for word in queryWords:
        if word in invertedIndex:
            matchedDocIDs.update(invertedIndex[word])
    if not matchedDocIDs:
        return []
    # Retrieve documents and their pageRank scores
    results = []
    for docID in matchedDocIDs:
        info = documentInfo[docID]
        results.append({
            'docID': docID,
            'url': info['url'],
            'title': info['title'],
            'description': info['description'],
            'pageRank': info['pageRank']
        })
    # Sort documents by pageRank score 
    sorted_results = sorted(results, key=lambda x: x['pageRank'], reverse=True)
    # Pagination
    start = (page - 1) * numResults
    end = start + numResults
    paginated_results = sorted_results[start:end]
    return paginated_results



# Load the inverted index and document info
# If you are using a different file, replace the path with the path to your file
# If you're using a database, replace this with the code to connect to your database
try:
    invertedIndex = load_invertedIndex('crawler/csv/invertedIndex.csv')
    documentInfo = load_documentInfo('crawler/csv/pageInfo.csv')
except FileNotFoundError:
    try:
        invertedIndex = load_invertedIndex('../csv/invertedIndex.csv')
        documentInfo = load_documentInfo('../csv/pageRank.csv')
    except FileNotFoundError:
        print('Error: Files not found')
        print('Exiting...')
        exit()

@app.route('/search')
def search_api():
    query = request.args.get('q', '')
    numResults = int(request.args.get('numResults', 10))
    page = int(request.args.get('page', 1))

    if not query:
        return jsonify({'error' : 'no query provided'}), 400
    results = search(query, invertedIndex, documentInfo, numResults=numResults, page=page)
    return jsonify({
        'query': query,
        'page': page,
        'numResults': numResults,
        'results': results
    })

if __name__ == '__main__':
    app.run(debug=True)