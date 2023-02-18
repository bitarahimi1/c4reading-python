# -*- coding: utf-8 -*-
'''
Note: run the following two commands in terminal before running this code to install the required libraries and download their required data.
'''
#pip install datasets spacy
#python -m spacy download en
from datasets import load_dataset
import spacy



def sentence_generator():
  nlp = spacy.load("en_core_web_sm")
  dataset = load_dataset('c4', 'en', streaming=True)
  global_counter = 0
  for url_counter, instance in enumerate(dataset['train']):
    text = instance['text']
    url = instance['url']
    doc = nlp(text)
    for local_counter, sentence in enumerate(doc.sents):
      sentence_id = url + '|||||' + str(url_counter) + '|||||' + str(global_counter) + '|||||' + str(local_counter)
      tokens = [token.text for token in nlp(sentence.text)]
      sentence_txt = ' '.join(tokens)
      yield sentence_id, sentence_txt
      global_counter += 1

if __name__ == '__main__':
  with open('./en/sentences', 'wt') as fout:
    for id, sentence in sentence_generator():
      fout.write(f"{id}\t{sentence}\n")
