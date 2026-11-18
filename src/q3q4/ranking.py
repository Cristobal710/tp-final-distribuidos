import heapq


# Clave con la que se ordenan las palabras de Q4: primero las que aparecen en mas articulos y, en caso de empate,
# por orden alfabetico. Sirve para tuplas (palabra, articulos) y para filas WordCount.
def rank_key(item):
    return -item[1], item[0]


# Devuelve las size mejores palabras ya ordenadas, sin ordenar todas: mantiene solo las candidatas que pueden entrar.
def best_words(candidates, size):
    return heapq.nsmallest(size, candidates, key=rank_key)
