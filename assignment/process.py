import collections
import re


def process_file():
    freq = collections.defaultdict(int)

    # read the script and store the frequency of each word
    with open("dummy.txt") as file:
        for line in file:

            # regular expression to extract only the words
            words = re.findall(r'\b[a-zA-Z]+\b', line)
            for word in words:
                freq[word.lower()] += 1

    return freq


process_file()
