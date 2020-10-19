import bz2
import re
import nltk

skwiki =  'data/skwiki-20201001-pages-articles.xml.bz2'
output = 'data/out.txt'
write = False;


stream = bz2.BZ2File(skwiki)
with open(output, 'wb') as f:
  for _ in range(1, 50000):#stream:
    line = stream.readline()
    if re.search(b'\</?page\>', line):
        write = not write
        f.write(b"\n\n=============================================\n\n")

    if(write):
        if(b"<title>" in line):
            f.write(re.sub(br'\<title\>(.*)\</title\>', br'\1', line))
        elif(b"<text" in line):
            text = b""
            text += line;
            while True:
                line = stream.readline()
                text += line
                if(b"</text>" in line):
                    text = re.sub(br'\[\[[^\]]+\|([^\]]+)\]\]', br'\1', text)
                    text = re.sub(br'\[\[([^\]]+)\]\]', br'\1', text)
                    text = re.sub(br'[\']{2,3}', br'', text)
                    #text = re.sub(br'\==[^=]*\==', br'', text)
                    #text = re.sub(br'[{{(\[\-\]{2})}}]', br'-', text)
                    #f.write(nltk.tokenize.sent_tokenize(text))
                    f.write(text)
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