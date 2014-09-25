t1: t1.c table.c
	cc t1.c table.c -I. -std=gnu99 -g -lhyperdex-client  -o t1
