class WordEstimate:
    
    """
    Генерирует списки стоп-слов на основе истории.
    На входе нужно дать два списка:
    1. список фраз, по которым не так все плохо
    2. список фраз, по которым ничего хорошего
    """
    
    def __init__ (self, goodkeys, badkeys):
        
        good_word_list = []
        for key in goodkeys:
            words = key.split(' ')
            for word in words:
                word = word.lower()
                if len(word) > 3:
                    good_word_list += [word]

        bad_word_list = []
        for key in badkeys:
            words = key.split(' ')
            for word in words:
                word = word.lower()
                if len(word) > 3 and \
                word not in good_word_list and\
                word not in bad_word_list:

                    bad_word_list += [word.lower()]    
        self.string_data = ', '.join(bad_word_list) 