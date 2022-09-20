# SAGE
SAGE is a system for large-scale sampling analysis of the uncertain network. SAGE performs graph analysis through sampling of uncertain networks. Sampling generates multiple sample networks, analyzes each sample network, and aggregates the results of each sample network. Large-scale sampling requires a large amount of memory resources. In order to support large-scale sampling, SAGE saves the data of the sample network to disk, and then loads the data of the sample network on demand for analysis. To optimize disk IO, SAGE provides optimization techniques.

## How to use
### Build
```sh
git clone https://github.com/mlsys-seo/sage
cd sage
mkdir build && cd build
cmake ../CMakeLists.txt
make -j
```
### Runing example alogrithm
1. Input graph formatting
```sh
build/tools/converter -t sample_input/facebook.txt -o sample_input/facebook.sage
```
2. Runing example alogrithm
```sh
build/algs/topk -f sample_input/facebook.sage -s 1000 -t 16 -m 3 -o c 128
```


## Publication
Eunjae Lee, Sam H. Noh, Jiwon Seo, SAGE: A System for Uncertain Network Analysis, in Proceedings of the International Conference on Very Large Data Bases (VLDB), 2022--2023.
