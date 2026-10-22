import re

WORD_REGEX = re.compile(r"\b\w+\b")
NUMBER_REGEX = re.compile(r"[+-]?((\d+\.?\d*)|(\.\d+))")


# Saca las palabras de un abstract en minusculas, sin stopwords ni numeros. Si una palabra aparece varias veces, se repite.
def extract_words(abstract, stop_words):
    if not isinstance(abstract, str):
        return []
    return [
        word for word in WORD_REGEX.findall(abstract.lower())
        if word not in stop_words and not NUMBER_REGEX.fullmatch(word)
    ]
