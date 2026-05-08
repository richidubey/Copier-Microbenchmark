all:
# 	gcc ./copier-main.c ./copier.c -o copier
# 	gcc ./normal.c -o normal
	gcc -O2 -std=gnu11 ./memcpy-bench.c ./copier.c -lm -o memcpy-bench
