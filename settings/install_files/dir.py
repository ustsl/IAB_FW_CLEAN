     """
            try:
                
                
                
            except:
                
                print ('Категория не найдена. Создаем')
                
                # определим имя директории, которую создаём
                
                oldpath = os.getcwd()
                absolutepath = oldpath + '/' + path
                
                try:
                    os.makedirs(absolutepath)
                except OSError:
                    print ("Создать директорию %s не удалось" % path)
                else:
                    print ("Успешно создана директория %s " % path)
                    df.to_excel(fullpath, sheet_name='er')
            """        