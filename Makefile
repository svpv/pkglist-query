RPM_OPT_FLAGS ?= -O2 -g -Wall
all: pkglist-query
pkglist-query: query.c
	$(CC) $(RPM_OPT_FLAGS) -pthread -fwhole-program -o $@ $< -lrpm -lzpkglist
