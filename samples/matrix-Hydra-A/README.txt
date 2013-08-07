README for Matrix Multiplication Application
============================================

Pre-Condition:
--------------
Assume you have already copy the Twister-Matrix-${Release}.jar into "apps" directory.

Generating Data:
----------------
./matrix_gen.sh  [n - height][m - width][file name]

e.g. ./matrix_gen.sh  1000 1000 matA.bin
     ./matrix_gen.sh  1000 1000 matB.bin

The matrices need not be in the same size. However, width of matrix A should match the height of matrix B.

Run Matrix Multiplication:
---------------------------

Once the above steps are successful you can simply run the following shell script to run Matrix Multiplication appliction.

./matrix_mult.sh [matrix A file name][matrix B file name][output file name][number of map tasks][number of iterations][block size]

e.g. ./matrix_mult.sh matA.bin matB.bin mat.out 40 10 128

Here, number of iterations divide the matrix A in row wise. Block size is an optimization to better utilze cache. It has nothing to do with the parallelization. typically use 128

After a while you should be able to see the timing infomration and smaller block of the output matrix printed on the console.

Note: Matrix multiplication is O(n^3) operation. So depending on the size of your matrices and the parallelizm selected it may take some noticable time. For example, 4000x4000 matrix multiplication took ~200 seconds on 40 CPU cors.


