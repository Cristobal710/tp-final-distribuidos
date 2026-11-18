import itertools

from q3q4.ranking import best_words


# Suma las filas de palabras guardadas: en cuantos articulos aparece cada una y si aparece en ingles y en frances.
def total_counts(rows):
    counts = {}
    for word, article_count, english, french in rows:
        entry = counts.get(word)
        if entry is None:
            counts[word] = [article_count, english, french]
        else:
            entry[0] += article_count
            entry[1] = entry[1] or english
            entry[2] = entry[2] or french
    return counts


# Resume lo que conto un shard recorriendo sus archivos de a uno, asi en memoria hay un solo archivo a la vez: va
# entregando las palabras compartidas (las que aparecen en ingles y en frances) y se queda con el top local.
# El top local esta completo cuando se termino de recorrer shared_words.
class ShardSummary:

    def __init__(self, top_size):
        self._top_size = top_size
        self.top = []

    # Recorre los conteos de cada archivo y devuelve las palabras compartidas, actualizando el top local.
    def shared_words(self, counts_per_file):
        for counts in counts_per_file:
            for word, (article_count, english, french) in counts.items():
                if english and french:
                    yield word
            candidates = itertools.chain(self.top, ((word, entry[0]) for word, entry in counts.items()))
            self.top = best_words(candidates, self._top_size)
