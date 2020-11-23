import bz2
import re
import nltk
import regex
import gzip
import heapq
import io
import time

##potential improvements, TODO:
# add a limit to the sentence length - long sentences are not fit for abstracts
# add a weight modifier to words retrieved from headings, description or infobox (maybe first sentence too?), also DESCRIPTION
# solve the issue with reading the gzip file line by line during every iteration - too slow - fixed, but something new is wrong now
# work on regexp, not every case is covered yet
# create and index, probably with elasticsearch

enwiki =  'data/enwiki-20201020-pages-articles.xml.bz2'
enabst = 'data/enwiki-20201020-abstract.xml.gz'
output = 'data/out.txt'
write = False;

lines = 1000000

stopwords=nltk.corpus.stopwords.words('english')

#functions that work with article content(text tag)


def clean_article(text):    
     #remove the text tag int he beggining of the article xml
    text = re.sub(br'<text.*>', br'', text)
    text = re.sub(br'[\']{2,3}', br'', text)
    #remove all citations - these are irrelevant for generating abstract
    text = re.sub(br'\{\{[cC]ite[^\]]+\|([^\}]+)\}\}', br'', text) 
    #need to remove tables, {{}} these things and so on
    #........
    #short description is important for us, replace it with a better format
    #text = re.sub(br'{{[sS]hort description\|(.*)}}', br'DESCRIPTION: \1.', text)
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
    text = re.sub(br'<big.*</big>', br'', text)
    text = re.sub(br'<math.*</math>', br'', text)
    text = re.sub(br'<ref.*>', br'', text)
    text = re.sub(br'&amp;', br'&', text)
    text = re.sub(br'nbsp;', br' ', text)
    text = re.sub(br'\\{1,}n{1,}', br'', text)
    #text = re.sub(br'\\{1,}', br'', text)
    return text

def extract_description(text, f):
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


def extract_headings(f, text, word_count):
    #extract all headings from the article, put them on the beggining and remove them from the rest of the article
    headings=re.findall(br'={2,}([^\=]+)={2,}', text)
    #f.write(b"\n<headings> ")
    for i in headings:
        word_count[i] = 3;
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


def add_weighted_keywords(word_count, words, weight):
    #will be used for adding weigthted words from: HEADINGS, DESCRIPTION, INFOBOX
    for word in words:
        if word not in word_count.keys():
                word_count[word] = weight
        else:
            word_count[word] += weight
    word_count = normalize_dict(word_count)
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
    sentences = nltk.sent_tokenize(str(text)) #utf-8 here fixes special characters, breaks everything else though...
    sent_score = {}
    for sentence in sentences:
        for word in nltk.word_tokenize(sentence):
            if word in word_count.keys():
                if sentence not in sent_score.keys():
                    sent_score[sentence] = word_count[word]
                else:
                    sent_score[sentence] += word_count[word]
        if len(sentence.split(' ')) > 20:
            sent_score[sentence] = sent_score[sentence] / 2
    return sent_score


def generate_article_abstract(sent_score, f, line_count, description):
    f.write(b"<my-abstract> ")
    sentence_ratio = int(line_count * 0.05)
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
        sent = sentence.encode('utf-8')#need to fix this somehow
        sent = str.encode(sentence, 'utf-8')
        sent = re.sub(rb'\\{1,}n{1,}', rb'', sent)
        sent = re.sub(rb'\\{1,}', rb'', sent)
        f.write(sent + b" ")
    f.write(b" </my-abstract>\n")
    
    
def find_matching_abstract(title, f, gz):
    write = False       
    text = b""
    #gz.seek(0, 1)
    #print("CURRENTLY AT " + str(gz.tell()) + "\n")
    #gz.seek(0, 1)
    #print("LOOKING FROM BYTE " + str(gz_pos) + "\n")
    for _ in range(1, lines):        
        line = gz.readline()
        if(line == b"<title>Wikipedia: " + title + b"</title>\n"):
            f.write(b"<wiki-abstract> ")
            while True:
                line = gz.readline()
                if b"<abstract>" in line:
                    write = True
                if write == True:
                    text+=line
                if b"</abstract>" in line:
                    f.write(re.sub(br'.*\<abstract\>(.*)\</abstract\>\n', br'\1', text))
                    f.write(b" </wiki-abstract>\n")
                    #print("CURRENTLY AT " + str(temp) + "\n" )
                    write = False
                    return
    f.write(b"<wiki-abstract> not found </wiki-abstract>\n")
    gz.seek(0)
    #failsafe, if abstract not found start from the beggining of the file
    return
       

def xml_wrap(text, tag):
    return (b"<" + tag + b"> " + text + b" <\\" + tag + b">")
            
#frequency table (special weight for infobox and headings)
#rank sentences
#construct abstract
#print abstract into a XML file (in XML form)
    

stream = bz2.BZ2File(enwiki, "rb")
with open(output, 'wb') as f, gzip.open(enabst, 'rb') as gz:
  f.write(b"<articles>\n")
  start = time.time()
  for _ in range(1, lines):#stream:
    line = stream.readline()
    if re.search(b'\</?page\>', line):
        write = not write

    if(write):
        if(b"<title>" in line):
            title = re.sub(br'.*\<title\>(.*)\</title\>\n', br'\1', line)
            if b" (disambiguation)" in title:
                #doesnt work
                continue
        elif(b"<text" in line):
            #remove redirects + disambiguation pages, generating abstracts for them is nonsensical
            #disambiguation is iffy TBH
            if b"#REDIRECT" in line or b"#redirect" in line: #or b"disambiguation" in line:
               continue
            f.write(b"<article>\n<title>"+ title + b"</title>\n")
            text = b""
            text += line;
            while True:
                line = stream.readline()
                #if any(x in line for x in matches):
                #    break
                if(b"</text>" in line):
                    find_matching_abstract(title, f, gz)
                    #print(curr_line)
                    text = parse_infobox(text)
                    text, description, word_count = extract_description(text, f)
                    text = clean_article(text)
                    text, word_count = extract_headings(f, text, word_count)
                    line_count = text.count(b'\n')
                    f.write(b"<lines> " + str.encode(str(line_count)) + b" </lines>\n")
                    #f.write(text)
                    generate_article_abstract(evaluate_sentences(text, calculate_keywords(text, word_count)), f, line_count, description)
                    f.write(b"</article>\n\n")
                    break
                text += line
  f.write(b"\n</articles>")
  gz.close()
  f.close()
  end = time.time()
  print("TIME ELAPSED: " + str(end - start) + "s")