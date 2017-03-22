from loremipsum import get_sentence

target = open('py_lorem.txt', 'w')

for i in range(100000/4):
    word_list = get_sentence().replace(' ', '\n').replace('.', '')
    target.write(word_list)

