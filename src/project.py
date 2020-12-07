import bz2
import re
import nltk
import regex
import gzip
import heapq
import time
import build_index
from elasticsearch import Elasticsearch 
from sklearn.feature_extraction.text import TfidfVectorizer

##potential improvements, TODO:
# add a limit to the sentence length - long sentences are not fit for abstracts
# add a weight modifier to words retrieved from headings, description or infobox (maybe first sentence too?), also DESCRIPTION
# solve the issue with reading the gzip file line by line during every iteration - too slow - fixed, but something new is wrong now
# work on regexp, not every case is covered yet
# create and index, probably with elasticsearch
# frequence analysis, keywords, stuff, ...
#nltk.download('stopwords')
#nltk.download('punkt')
enwiki =  '../data/enwiki-20201020-pages-articles.xml.bz2'
enabst = '../data/enwiki-20201020-abstract.xml.gz'
write = False;

lines = 10000
#for 1 000 000 lines, the program runs for 30 minutes

stopwords=nltk.corpus.stopwords.words('english')

#functions that work with article content(text tag)

def insert(ID, title, wikiAbstract, myAbstract, keywords, similarity):
    #insert the document into elasticsearch in json format (indexing)
    payload = {
            "title": title,
            "wiki-abstract": wikiAbstract,
            "my-abstract": myAbstract,
            "similarity": similarity,
            "keywords": keywords
    }
    es.index(index='abstract',doc_type='test',id=ID, body=payload)


def clean_article(text):    
     #remove the text tag int he beggining of the article xml
    text = re.sub(br'<text.*>', br'', text)
    #remove images ([[File]] - regexp from https://stackoverflow.com/questions/34717096/how-to-remove-all-files-from-wiki-string/34751544#34751544
    text = re.sub(br'\[\[[Ff]ile:[^[\]]*(?:\[\[[^[\]]*]][^[\]]*)*]]', br'', text)
    text = re.sub(br'\[\[:[fF]ile:.*\]\]', br'', text)
    text = re.sub(br'\[\[[fF]ile:.*\]\]', br'', text)
    text = re.sub(br'File:.*', br'', text)
    text = re.sub(br'[\']{2,3}', br'', text)
    #remove all citations - these are irrelevant for generating abstract
    text = re.sub(br'\{\{[cC]ite[^\]]+\|([^\}]+)\}\}', br'', text) 
    #need to remove tables, {{}} these things and so on
    text = re.sub(br'\{\| class=\"wikitable[^\\}]+\|\}', br'', text) 

    #short description is important for us, replace it with a better format
    #text = re.sub(br'{{[sS]hort description\|(.*)}}', br'DESCRIPTION: \1.', text)
    #remove {{}} constructions, we already parsed all that are relevant to us
    text = re.sub(br'\{\{([^\}]+)\}\}', br'', text)
    #remove all tables
    text = re.sub(br'\{\|([^\}]+)\|\}', br'', text)
    #remove all comments
    text = re.sub(br'&lt;!--[^\-]+--&gt;', br'', text)
    
    
    text = re.sub(br'\[\[[^\]]+\|([^\]]+)\]\]', br'\1', text)
    text = re.sub(br'\[\[([^\]]+)\]\]', br'\1', text)
    
    text = re.sub(br'&quot;', br'"', text)
    text = re.sub(br'Category:[^\n]*', br'', text)
    text = re.sub(br'^[*|!:&][^\n]*', br'', text, flags = re.MULTILINE)
    text = re.sub(br'\n{2,}', br'\n', text)
    text = re.sub(br'&lt;', br'<', text)
    text = re.sub(br'&gt;', br'>', text)
    text = re.sub(br'<ref.*</ref>', br'', text)
    text = re.sub(br'<sub.*</sub>', br'', text)
    text = re.sub(br'<big.*</big>', br'', text)
    text = re.sub(br'<math.*</math>', br'', text)
    text = re.sub(br'<ref.*>', br'', text)
    text = re.sub(br'&amp;', br'&', text)
    text = re.sub(br'nbsp;', br' ', text)
    text = re.sub(br'\\{1,}n{1,}', br'', text)
    #text = re.sub(br'\\"', br'"', text)
    #text = re.sub(br'\\{1,}', br'', text)
    return text

def extract_description(text):
    #short description is important for us, replace it with a better format
    word_count = {}
    result = re.findall(br'{{[sS]hort description\|(.*)}}', text)
    text = re.sub(br'{{[sS]hort description\|(.*)}}', br'', text)
    if result:
        ret = (result[0] + b".").capitalize()
        words = str(ret, 'utf-8').split(' ')
        for i in words:
            if i not in stopwords:
                word_count[i] = 5;
    else:
        ret = None
    return text, ret, word_count


def parse_infobox(text):
    text = regex.sub(br'(?=\{[iI]nfobox)(\{([^{}]|(?1))*\})', br' ', text)
    return text


def extract_headings(text, word_count):
    #extract all headings from the article, put them on the beggining and remove them from the rest of the article
    headings=re.findall(br'={2,}([^\=]+)={2,}', text)
    #f.write(b"\n<headings> ")
    for i in headings:
        temp = i.split(b' ')
        for j in temp:
            if j not in stopwords:
                word_count[str(j, 'utf-8')] = 3;
        #f.write(i + b", ")
    #f.write(b"</headings>\n\n")
    text = re.sub(br'={2,}([^\=]+)={2,}', br'', text)
    #return the modified text + word_count dictionary with preset values for headings
    return text, word_count
 
    
def calculate_keywords(article_content, word_count):
    #word_count = {}
    for word in nltk.word_tokenize(str(article_content)):
        if word not in stopwords:
            word = re.sub(r'\\{1,}n{1,}', r'', word)
            word = re.sub(r'\\{1,}', r'', word)
            if word not in word_count.keys():
                word_count[word] = 1
            else:
                word_count[word] += 1
    word_count = normalize_dict(word_count)
    #print({k: v for k, v in sorted(word_count.items(), key = lambda item: item[1])})
    return word_count

def normalize_dict(word_count):
    word_count.pop(';', None)
    word_count.pop(',', None)
    word_count.pop('.', None)
    word_count.pop('(', None)
    word_count.pop(')', None)
    word_count.pop('<', None)
    word_count.pop('>', None)
    word_count.pop('?', None)
    word_count.pop('!', None)
    word_count.pop('&', None)
    word_count.pop('\""', None)
    word_count.pop('\'', None)
    word_count.pop('{', None)
    word_count.pop('}', None)
    word_count.pop('\n', None)
    word_count.pop('``', None)
    word_count.pop('\'\'', None)
    word_count.pop('\'s', None)
    word_count.pop('', None)
    word_count.pop(' ', None)
    return word_count


def evaluate_sentences(text, word_count):
    sentences = nltk.sent_tokenize(str(text, 'utf-8')) #utf-8 here fixes special characters
    sent_score = {}
    for sentence in sentences:
        for word in nltk.word_tokenize(sentence):
            if word in word_count.keys():
                if sentence not in sent_score.keys():
                    sent_score[sentence] = word_count[word]
                else:
                    sent_score[sentence] += word_count[word]
        if len(sentence.split(' ')) > 30:
            #long sentences are not fit for abstracts
            sent_score[sentence] = -1
        elif len(sentence.split(' ')) < 2:
            #short sentences dont have enough information
            sent_score[sentence] = -1
    return sent_score


def generate_article_abstract(sent_score, line_count, description):
    #f.write(b"<my-abstract> ")
    sentence_ratio = int(line_count * 0.05)
    result = ""
    #set both upper and lower limit
    if sentence_ratio == 0:
        sentence_ratio = 1
    elif sentence_ratio > 5:
        sentence_ratio = 5
    
    abstract = heapq.nlargest(sentence_ratio, sent_score, key = sent_score.get)
    #if description:
    #    f.write(description + b". ")
    if description:
        abstract.insert(0, str(description, 'utf-8'))
    for sentence in abstract:
        #sent = sentence.encode('utf-8')#need to fix this somehow
        #sent = str.encode(sentence, 'utf-8')
        sent = sentence;
        sent = re.sub(r'\\{1,}n{1,}', r'', sent)
        sent = re.sub(r'\\{1,}', r'', sent)
        sent = sent.strip('\n')
        sent = sent.strip('\t')
        result += sent + " "
        #f.write(sent + b" ")
    #f.write(b" </my-abstract>\n")
    return ' '.join(result.split()), sentence_ratio
    
    
def find_matching_abstract(title, gz):
    write = False       
    text = b""
    pos = gz.tell()
    #gz.seek(0, 1)
    #print("CURRENTLY AT " + str(gz.tell()) + "\n")
    #gz.seek(0, 1)
    #print("LOOKING FROM BYTE " + str(gz_pos) + "\n")
    for _ in range(1, lines):        
        line = gz.readline()
        if(line == b"<title>Wikipedia: " + title + b"</title>\n"):
            #f.write(b"<wiki-abstract> ")
            while True:
                line = gz.readline()
                if b"<abstract>" in line:
                    write = True
                if write == True:
                    text+=line
                if b"</abstract>" in line:
                    #f.write(re.sub(br'.*\<abstract\>(.*)\</abstract\>\n', br'\1', text))
                    #f.write(b" </wiki-abstract>\n")
                    #print("CURRENTLY AT " + str(temp) + "\n" )
                    write = False
                    return re.sub(br'.*\<abstract\>(.*)\</abstract\>\n', br'\1', text)
    #f.write(b"<wiki-abstract> not found </wiki-abstract>\n")
    gz.seek(pos)
    return b"not found"
    #gz.seek(0)
    #failsafe, if abstract not found start from the beggining of the file
    #return
    
def calculate_simillarity(doc1, doc2):
    corpus = []
    corpus.append(doc1)
    corpus.append(doc2)
    try:
        vect = TfidfVectorizer(min_df=1, stop_words="english")                                                                                                                                                                                                   
        tfidf = vect.fit_transform(corpus)                                                                                                                                                                                                                       
        similarity = tfidf * tfidf.T 
        return similarity.toarray()[0][1]
    except:
        return 0
       
stream = bz2.BZ2File(enwiki, "rb")
with gzip.open(enabst, 'rb') as gz:
  disambiguationCount = 0
  redirectCount = 0
  articleCount = 0
  similaritySum = 0
  sentenceSum = 0
  exactMatchCount = 0
  noMatchCount = 0
  #build a new ES index
  build_index.build()
  #connect to ES
  es=Elasticsearch([{'host':'localhost','port':9200}])
  #f.write(b"<articles>\n")
  start = time.time()
  for ID in range(1, lines):#stream:
    line = stream.readline()
    if re.search(b'\</?page\>', line):
        write = not write

    if(write):
        if(b"<title>" in line):
            title = re.sub(br'.*\<title\>(.*)\</title\>\n', br'\1', line)
            if re.search(b'(?i)disambiguation', title) or re.search(b'(?i)Wikipedia:', title):
               #print("WE HAVE A HIT")
                if b'disambiguation' in title:
                    disambiguationCount += 1
                while True:
                    line = stream.readline()
                    if re.search(b'\</?page\>', line):
                        write = not write
                        break
                continue
        elif(b"<text" in line):
            #remove redirects + disambiguation pages, generating abstracts for them is nonsensical
            #disambiguation is iffy TBH
            #if b"#REDIRECT" in line or b"#redirect" in line: #or b"disambiguation" in line:
            if re.search(b'(?i)#redirect', line):
                redirectCount += 1
                while True:
                    line = stream.readline()
                    if re.search(b'\</?page\>', line):
                        write = not write
                        break
                continue
            #f.write(b"<article>\n<title>"+ title + b"</title>\n")
            text = b""
            text += line;
            while True:
                if re.search(b'(?i)refer to:', line) or re.search(b'(?i)refers to:', line) or re.search(b'(?i)stand for:', line):
                    disambiguationCount += 1
                    while True:
                        line = stream.readline()
                        if re.search(b'\</?page\>', line):
                            write = not write
                            break
                    break
                line = stream.readline()
                if(b"</text>" in line):
                    #f.write(text)
                    try:
                        #main pipeline, in try except block because we dont want the program to crash - it runs for ages, if we encounter an exception we simply skip the article
                        articleCount += 1
                        wikiAbstract = find_matching_abstract(title, gz)
                        #wikiAbstract = b"test"
                        #print(curr_line)
                        text = parse_infobox(text)
                        text, description, word_count = extract_description(text)
                        text = clean_article(text)
                        text, word_count = extract_headings(text, word_count)
                        line_count = text.count(b'\n')
                        #f.write(b"<lines> " + str.encode(str(line_count)) + b" </lines>\n")
                        #f.write(text)
                        myAbstract, sentences = generate_article_abstract(evaluate_sentences(text, calculate_keywords(text, word_count)), line_count, description)
                        sentenceSum += sentences
                        sim = calculate_simillarity(wikiAbstract, myAbstract)
                        if sim == 1:
                            exactMatchCount += 1
                        elif sim == 0:
                            noMatchCount += 1
                        similaritySum += sim
                        insert(ID, str(title, 'utf-8'), str(wikiAbstract, 'utf-8'), myAbstract, heapq.nlargest(3, word_count, key = word_count.get), sim);
                        #f.write(b"</article>\n\n")
                        break
                    except:
                        #if an error occurs, we dont want the program to crash...
                        break
                text += line
  #f.write(b"\n</articles>")
  gz.close()
  #f.close()
  end = time.time()
  #print frequency analysis results
  print("\n\nFrequency analysis\n")
  print("Disambiguation pages: " + str(disambiguationCount) + "\nRedirect pages: " + str(redirectCount) + "\nTotal articles / abstracts generated: " + str(articleCount))
  print()
  print("Average abstract length is: " + str(sentenceSum / articleCount) + " sentences")
  print("Average abstract similarity is: " + str(similaritySum / articleCount))
  print()
  print("Number of exact abstract matches: " + str(exactMatchCount))
  print("Exact abstract match percentage: " + str(exactMatchCount / articleCount * 100) + "%")
  print("Number of entirely different abstracts: " + str(noMatchCount))
  print("Entirely new abstract percentage: " + str(noMatchCount  / articleCount * 100) + "%")
  print()
  print("TIME ELAPSED: " + str(end - start) + " s")