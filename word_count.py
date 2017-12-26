# -*- coding:utf8 -*- 
from __future__ import print_function
import numpy as py
import re
import sys

factor1 = 9
factor2 = 1

word_dicts = {}
total_words = 0
all_words = []

class Wordclass:
	def __init__(self , word , prob):
		self.word = word 
		self.prob = prob
	def __repr__(self):
		return repr((self.word,self.prob))

def init():
	tot = 0
	for t in range(30):
		if(t < 10):
				filename = 'output_wordcount/part-r-0000' + str(t)
		else :
			filename = 'output_wordcount/part-r-000' + str(t)
		fr = open(filename,'r',encoding='utf8')
		data = fr.readlines()
		lines = len(data)
		for cdata in data:
			cdata = cdata.strip()
			clss = cdata.split('	')
			word_dicts[clss[0]] = tot
			all_words.append(clss[1])
			tot += 1
	return tot , all_words , word_dicts


def Prediction(target_words):

	target_word = target_words[0] + ''
	check = False

	if(len (target_words) == 1):
		check = True

	word_dict = {}
	word_list = []

	if (check == False):
		word_times = all_words[word_dicts[target_words[0]]].split('/')
		sum1 = 0 
		s2 = 0 
		for i in range(len(word_times)):
			word_times_part = word_times[i].split('-')
			if(target_words in word_times_part[0]):
				if(word_times_part[0] == target_words):
					sum1 = int(word_times_part[1])
		for i in range(len(word_times)):
			word_times_part = word_times[i].split('-')
			if(target_words in word_times_part[0]):
				if(target_words != word_times_part[0]):
					number1 = int(word_times_part[1])
					prob = float(number1) / float(sum1)
					prob *= factor1
					if(word_times_part[0][2] not in word_dict):
						word_dict[word_times_part[0][2]] = len(word_list)
						word_list.append(Wordclass(word_times_part[0][2] , prob))
					else :
						word_list[word_dict[word_times_part[0][2]]].prob += prob
		word_times = all_words[word_dicts[target_words[1]]].split('/')
		sum1 = 0 
		sum2 = 0 
		s2 = 0 
		for i in range(len(word_times)):
			word_times_part = word_times[i].split('-')
			if(len(word_times_part[0]) == 2):
				sum1 += int (word_times_part[1])
			if(len(word_times_part[0]) == 3):
				sum2 += int (word_times_part[1])
		for i in range(len(word_times)):
			word_times_part = word_times[i].split('-')
			if(len(word_times_part[0]) > 1):
				number1 = int(word_times_part[1])
				prob = 0 
				words = ''
				if(len(word_times_part[0]) == 2):
					words = word_times_part[0][1]
					prob = float(number1) / float(sum1)
				else :
					words = word_times_part[0][1] + word_times_part[0][2]
					prob = float(number1) / float(sum2)
					prob *= factor2
				if(words not in word_dict):
					word_dict[words] = len(word_list)
					word_list.append(Wordclass(words, prob))
				else :
					word_list[word_dict[words]].prob += prob	

	if(check == True) :
		word_times = all_words[word_dicts[target_words[0]]].split('/')
		sum1 = 0 
		sum2 = 0 
		s2 = 0
		for i in range(len(word_times)):
			word_times_part = word_times[i].split('-')
			if(len(word_times_part[0]) == 2):
				sum1 += int (word_times_part[1])
			if(len(word_times_part[0]) == 3):
				sum2 += int (word_times_part[1])
			for i in range(len(word_times)):
				word_times_part = word_times[i].split('-')
				if(len(word_times_part[0]) > 1):
					number1 = int(word_times_part[1])
					prob = 0 
					words = ''
					if(len(word_times_part[0]) == 2):
						words = word_times_part[0][1]
						prob = float(number1) / float(sum1)
					else :
						words = word_times_part[0][1] + word_times_part[0][2]
						prob = float(number1) / float(sum2)
						prob *= (factor1 + factor2)
					if(words not in word_dict):
						word_dict[words] = len(word_list)
						word_list.append(Wordclass(words, prob))
					else :
						word_list[word_dict[words]].prob += prob

	word_list = sorted(word_list , key = lambda w : w.prob , reverse = True)

	return word_list

init()

print ("----------------输入汉字----------------")
target_words = input()



result = target_words + ''

word = ''

if(len (target_words) >= 2):
	word = target_words[len(target_words) - 2] + target_words[len(target_words) - 1] + ''
else :
	word = target_words[0] + ''


#print (all_words[15])

#Prediction(word)


while True:
	word_list = Prediction(word)
	for i in range(10):
		print (i + 1,'-', word_list[i].word , '  ' , end = '')
		if((i + 1) % 5 == 0):
			print()
	choice = int (input())
	if(choice == 0):
		break 
	else :
		result += word_list[choice - 1].word 
		print(result)
		if (len(word_list[choice - 1].word) == 2):
			word = word_list[choice - 1].word
		else :
			if(len(word) == 1):
				word = word[0] + word_list[choice - 1].word + ''
			else :
				word = word[1] + word_list[choice - 1].word + ''



