import bz2
import re
import nltk
import regex
import gzip
import heapq



#skwiki =  'data/skwiki-20201001-pages-articles.xml.bz2'
enwiki =  'data/enwiki-20201020-pages-articles.xml.bz2'
enabst = 'data/enwiki-20201020-abstract.xml.gz'
output = 'data/out.txt'
write = False;

lines = 100000

stopwords=nltk.corpus.stopwords.words('english')

#functions that work with article content(text tag)

def clean_article(text):    
    text = re.sub(br'[\']{2,3}', br'', text)
    #remove all citations - these are irrelevant for generating abstract
    text = re.sub(br'\{\{[cC]ite[^\]]+\|([^\}]+)\}\}', br'', text) 
    #need to remove tables, {{}} these things and so on
    #........
    #short description is important for us, replace it with a better format
    text = re.sub(br'\{\{[sS]hort description\|([^\}]+)\}\}', br'\1', text)
    #remove the text tag int he beggining of the article xml
    text = re.sub(br'\<([^\>]+\>)', br'', text)
    #remove {{}} constructions, we already parsed all that are relevant to us
    text = re.sub(br'\{\{([^\}]+)\}\}', br'', text)
    #remove all tables
    text = re.sub(br'\{\|([^\}]+)\|\}', br'', text)
    #remove all comments
    text = re.sub(br'&lt;!--[^\-]+--&gt;', br'', text)
    #remove images ([[File]] - regexp from https://stackoverflow.com/questions/34717096/how-to-remove-all-files-from-wiki-string/34751544#34751544
    text = re.sub(br'\[\[[Ff]ile:[^[\]]*(?:\[\[[^[\]]*]][^[\]]*)*]]', br'', text)
    
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
    text = re.sub(br'<ref.*>', br'', text)
    text = re.sub(br'&amp;', br'&', text)
    text = re.sub(br'nbsp;', br' ', text)
    text = re.sub(br'\\{1,}n{1,}', br'', text)
    text = re.sub(br'\\{1,}', br'', text)
    return text

def parse_infobox(text):
    text = regex.sub(br'(?=\{[iI]nfobox)(\{([^{}]|(?1))*\})', br'INFOBOX', text)
    return text

def extract_headings(f, text):
    #extract all headings from the article, put them on the beggining and remove them from the rest of the article
    headings=re.findall(br'={2,}([^\=]+)={2,}', text)
    f.write(b"\n<headings> ")
    for i in headings:
        f.write(i + b" , ")
    f.write(b"</headings>\n\n")
    text = re.sub(br'={2,}([^\=]+)={2,}', br'', text)
    return text
 
def calculate_keywords(article_content):
    word_count = {}
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
    return word_count




def evaluate_sentences(text, word_count):
    sentences = nltk.sent_tokenize(str(text))
    sent_score = {}
    for sentence in sentences:
        for word in nltk.word_tokenize(sentence):
            if word in word_count.keys():
                if sentence not in sent_score.keys():
                    sent_score[sentence] = word_count[word]
                else:
                    sent_score[sentence] += word_count[word]
    return sent_score

def generate_article_abstract(sent_score, f):
    f.write(b"<my-abstract> ")
    abstract = heapq.nlargest(3, sent_score, key = sent_score.get)       
    for sentence in abstract:
        sent = str.encode(sentence)
        sent = re.sub(rb'\\{1,}n{1,}', rb'', sent)
        sent = re.sub(rb'\\{1,}', rb'', sent)
        f.write(sent)
    f.write(b" </my-abstract>\n")
    
def find_matching_abstract(title, f):
    abstract_stream = gzip.open(enabst, 'rb')
    write = False       
    text = b""
    for _ in range(1, lines):        
        line = abstract_stream.readline()
        if(line == b"<title>Wikipedia: " + title + b"</title>\n"):
            f.write(b"<wiki-abstract> ")
            while True:
                line = abstract_stream.readline()
                if b"<abstract>" in line:
                    write = True
                if write == True:
                    text+=line
                if b"</abstract>" in line:
                    f.write(re.sub(br'.*\<abstract\>(.*)\</abstract\>\n', br'\1', text))
                    f.write(b" </wiki-abstract>\n")
                    return
    f.write(b"<wiki-abstract> not found </wiki-abstract>")
            
    
    
#frequency table (special weight for infobox and headings)
#rank sentences
#construct abstract
#print abstract into a XML file (in XML form)
    

stream = bz2.BZ2File(enwiki, "rb")
with open(output, 'wb') as f:
  for _ in range(1, lines):#stream:
    line = stream.readline()
    if re.search(b'\</?page\>', line):
        write = not write
        #f.write(b"\n\n=============================================\n\n")

    if(write):
        if(b"<title>" in line):
            title = re.sub(br'.*\<title\>(.*)\</title\>\n', br'\1', line)
        elif(b"<text" in line):
            if b"#REDIRECT" in line:
               continue
            f.write(b"\n\n=============================================\n\n<title>"+ title + b"</title>\n")
            text = b""
            text += line;
            while True:
                
                line = stream.readline()
                #if any(x in line for x in matches):
                #    break
                text += line
                if(b"</text>" in line):
                    find_matching_abstract(title, f)
                    text = clean_article(text)
                    text = extract_headings(f, text)
                    generate_article_abstract(evaluate_sentences(text, calculate_keywords(text)), f)
                    text = parse_infobox(text)
                    #calculate_keywords(text)
                    #split_by_sentences(text)
                    #text = re.sub(br'\==[^=]*\==', br'', text)
                    #text = re.sub(br'[{{(\[\-\]{2})}}]', br'-', text)
                    #f.write(nltk.tokenize.sent_tokenize(text))
                    #' '.join(str(text).split())
                    #pattern = re.compile(r'\s+')
                    #re.sub(pattern, ' ', str(text))
                    #f.write(text+b"\n\n=============================================\n\n")
                    break
                

            
            
            

#import re
#import nltk
#nltk.download('punkt')
#import nltk.tokenize

#data = "Testujem. Toto je test. Opakujem, toto je test. Test test test test. tesT?"
#sent = nltk.tokenize.sent_tokenize(data)
#print(sent)
#for i in sent:
#    print(nltk.tokenize.word_tokenize(i))

#pattern = re.compile("<[title]>(​.+?)</[title]>")

#for i, line in enumerate(open('data/Newton.txt')):
#    for match in re.finditer(pattern, line):
#        print('Found on line %s: %s' % (i+1, match.group()))