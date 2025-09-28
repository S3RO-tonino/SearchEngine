from bs4 import BeautifulSoup as bs
import requests
import time
import random
from queue import Queue
import threading
from urllib.parse import *
from concurrent.futures import ThreadPoolExecutor



def parse_robots_from_url(robotsURL):
    current_agent = None
    header = {"User-Agent": "simple_crawler/0.1"}
    rules = {}

    response = requests.get(robotsURL, headers=header, timeout=5)
    response.raise_for_status()

    for line in response.text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):  # ignora righe vuote e commenti
            continue

        if line.lower().startswith("user-agent:"):
            current_agent = line.split(":", 1)[1].strip()

        elif line.lower().startswith("disallow") and current_agent:
            disallow_value = line.split(":", 1)[1].strip()
            # se l'agent ha più di una regola, accumulale
            rules.setdefault(current_agent, []).append(disallow_value)

    return rules



def can_crawl(url):
    userAgent = 'simple_crawler'
    parsedURL = urlparse(url)
    robotsURL = f"{parsedURL.scheme}://{parsedURL.netloc}/robots.txt"
    
    print(f"Checking robots.txt for: {robotsURL}")
    time.sleep(random.uniform(1, 3))

    try:
        rules = parse_robots_from_url(robotsURL)

        if (rules.get('*') and parsedURL.path in rules.get('*') or rules.get(userAgent) and parsedURL.path in rules.get(userAgent)):
            print(f'Disallowed by robots.txt: {url}')
            return False

        else:
            return True
                
    except requests.RequestException as e:
        print(f'failed to access robots.txt: {robotsURL} - {e}')
        return False # File robots.txt non presente presupponiamo non sia permesso crawling



def parse_link(hyperlinks, currentUrl):
    urls = []
    
    for hyperlink in hyperlinks:
        url = hyperlink["href"]

        # Format the URL into a proper URL
        if url.startswith("#"):
            continue  # Skip same-page anchors
        if url.startswith("//"):
            url = "https:" + url  # Add scheme to protocol-relative URLs
        elif url.startswith("/"):
            # Construct full URL for relative links
            base_url = "{0.scheme}://{0.netloc}".format(requests.utils.urlparse(currentUrl))
            url = base_url + url
        elif not url.startswith("http"):
            continue  # Skip non-HTTP links
        url = url.split("#")[0]  # Remove anchor
        urls.append(url)
    return urls



def crawl(args):
    queue = args['queue']
    visitedUrls = args['visitedUrls']
    crawlCount = args['crawlCount']
    CRAWL_LIMIT = args['CRAWL_LIMIT']
    lock = args['lock']
    index = args['index']
    webpageInfo = args['webpageInfo']
    webpageIDCounter = args['webpageIDCounter']
    stopCrawl = args['stopCrawl']

    header = {"User-Agent": "simple_crawler/0.1"}
    
    while not stopCrawl.is_set():
        try:
            currentUrl = queue.get(timeout=5)
            print("Time to crawl: " + currentUrl)
        except Exception as e:
            break

        with lock:
            if crawlCount[0] >= CRAWL_LIMIT:
                print("Crawl limit reached. Exiting...")
                queue.queue.clear()
                stopCrawl.set()
                break
            if currentUrl in visitedUrls:
                print(f'URL in visited list {url}')
                queue.task_done()
                continue
            visitedUrls.add(currentUrl)

        if not can_crawl(currentUrl):
            print(f'Cannot crawl {currentUrl}')
            queue.task_done()
            continue

        time.sleep(random.uniform(2, 4))

        try:
            response = requests.get(currentUrl, headers=header)
            response.raise_for_status()

            # check for noindex TODO capire perché e cosa significa questo noindex directive
            if 'noindex' in response.content.decode('utf-8').lower():
                print(f'No index found - Skipping: {currentUrl}')
                queue.task_done()
                continue
            
            wp = bs(response.content, "html.parser") # parsing response to find new URLs
            hyperlinksFound = wp.select("a[href]")

            newUrls = parse_link(hyperlinksFound, currentUrl) # get and normalize the URLs from the 'a' html tags

            with lock:
                for url in newUrls:
                    # print(f'\n[DEBUG] URL found: {url}')
                    if url not in visitedUrls:
                        queue.put(url) # add the new url to the queue
                crawlCount[0] += 1

        except requests.RequestException as e:
            print(f'Failed to fetch {currentUrl} : {e}')

        finally:
            queue.task_done()



def crawl_bot():
    startingUrls = [
        'https://it.wikipedia.org/wiki/Pagina_principale',
    ]
    
    urlsToCrawl = Queue()

    for seedUrl in startingUrls:
        urlsToCrawl.put(seedUrl)

    visitedUrls = set()
    CRAWL_LIMIT = 10
    crawlCount = [0]
    lock = threading.Lock()
    stopCrawl = threading.Event()

    # indexing section
    index = {}
    webpageInfo = {}
    webpageIDCounter = {}

    # Start concurrent crawling w ThreadPoolExecutor
    NUM_WORKERS = 50

    args = {
        'queue' : urlsToCrawl,
        'visitedUrls' : visitedUrls,
        'crawlCount' : crawlCount,
        'CRAWL_LIMIT' : CRAWL_LIMIT,
        'lock' : lock,
        'index' : index,
        'webpageInfo' : webpageInfo,
        'webpageIDCounter' : webpageIDCounter,
        'stopCrawl' : stopCrawl
    }


    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for _ in range(NUM_WORKERS):
            executor.submit(crawl, args)
        
        
        """
        print('\n\n\n')
        print('All URLs have been crawled.')
        print(f'CRAWL finished in: {round(time.time() - start, 3)} - CRAWLED: {crawlCount}')
        """


    # Indexing part - TODO Decidere se salvare i dati su DB o excel



def main():
    crawl_bot()



if __name__ == "__main__":
    main()
