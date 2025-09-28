import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer




def download_nltk_resources():
    try:
        stopwords.words('english')
    except LookupError:
        nltk.download('stopwords')
    try:
        word_tokenize('test')
    except LookupError:
        nltk.download('punkt')



def indexPage(wp, url):
    download_nltk_resources()
    stopWords = set(stopwords.words('english'))
    ps = PorterStemmer()

    titleTag = wp.find('title')
    title = titleTag.getText().strip() if titleTag else 'No title'

    description = ''
    metaDescription = wp.find('meta', attrs={'name': 'description'})
    if metaDescription and 'content' in metaDescription.attrs:
        description = metaDescription['content']
    else:
        textContent = wp.get_text(separator=" ", strip=True)
        description = textContent[:200] + "..." if len(textContent) > 200 else textContent

    # Tokenize and filter words
    textContent = wp.get_text(separator=' ', strip=True)
    tokens = word_tokenize(textContent.lower())
    filteredWords = [ ps.stem(word) for word in tokens if word.isalpha() and word not in stopWords ]

    indexedPage = {
        "url": url,
        "title": title,
        "description": description,
        "words": filteredWords
    }

    print(f'Indexed: {url} - INFO: title {title} - number of words: {len(filteredWords)}')

    return indexedPage