def dimension_func(dictdimensions):
    """
    Функция вытаскивает DIMENSIONS из строки и отдает часть листа
    Работает для DictToListFromYandexMetrika
    """
    listdimensions = []
    for dimension in dictdimensions:
        listdimensions += [dimension['name']]
    return listdimensions


def yametrica_dictlistfunc (data):
    """
    Функция принимает словарь данных и отдает в лист.
    """
    list_data = [] #Сюда складываем весь массив данных системы
    for element in data:
        list_data +=  [dimension_func(element['dimensions']) + element['metrics']]
    return list_data