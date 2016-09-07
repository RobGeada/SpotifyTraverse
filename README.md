# SpotifyTraverse

With SpotifyTraverse, you can find the shortest paths between any two artists in Spotify's database, using the "Related Artists" functionality to define connections. Included are four different sized datasets, each including the n-most popular artists on Spotify up to a variable threshold. As such, the smaller datasets provide greater traversal speed at the expense of neglecting smaller, lesser-known artists. Larger databases take longer to compute, but are more likely to provide a path that is closer to the 'optimal' route. 

###Prerequisites
* Inquirer
* Apache Spark 2.0.0
* Python 2.7

###Usage
Simply run `python SpotifyTraverse.py` and let the UI take care of the rest!
