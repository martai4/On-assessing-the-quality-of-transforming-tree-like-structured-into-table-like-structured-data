import random

import client_airlines
import client_movies
import client_nasa
import client_gists
import client_reddit
import client_hierarchical

LOOPS = 1

if __name__ == '__main__':
    random.seed(23)
    for _ in range(LOOPS):
        client_airlines.airlines_client()
        client_movies.client_movies()
        client_nasa.client()
        client_gists.client_gists()
        client_reddit.client_reddit()
        # hierarchical
        client_hierarchical.client_movies()
        client_hierarchical.client_airlines()
        client_hierarchical.client_nasa()
        client_hierarchical.client_gists()
        client_hierarchical.client_reddit()
