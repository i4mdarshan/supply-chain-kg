import spacy
nlp = spacy.load('en_core_web_lg')
print('spaCy ready:', nlp.meta['name'])


"""
Installing collected packages: en-core-web-lg
Expected output:

spaCy ready: en-core-web-lg-3.8.0


"""