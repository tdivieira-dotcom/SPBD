import string
from unidecode import unidecode # used to remove accents and such...

def word_counting(filename):
      dictionary={}
      with open(filename, 'r') as f:
        for line in f:
          line=line.lower()
          line=unidecode(line).translate(line.maketrans('','',string.punctuation+'«»'))
          words=line.split()
          for word in words:
            dictionary[word]=dictionary.get(word,0) + 1
      return dictionary



filename="os_maias.txt"
for word,frequency in sorted(word_counting(filename).items()):
    print(f"{word}:{frequency}")
