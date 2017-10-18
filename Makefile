RPM_OPT_FLAGS ?= -O2 -g -Wall
all: pkglist-query
pkglist-query: query.c
	$(CC) $(RPM_OPT_FLAGS) -pthread -fwhole-program -o $@ $< -lrpm -lzpkglist
ALT = /ALT
REPO = $(ALT)/Sisyphus/noarch
COMP = classic
RPMS = $(REPO)/RPMS.$(COMP)
# This query is executed in hasher.
NVRA = %{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}
Q1 = [%{FILENAMES}\t%{NAME}\t$(NVRA)\n]
Q2 = [%{PROVIDENAME}\t%{PROVIDENAME}\t$(NVRA)\n]
out1.lz4: $(RPMS)
	find $< -name '*.rpm' -execdir \
	rpmquery --qf '$(Q1)$(Q2)' -p -- {} + | \
	sort -u |lz4 >$@
pkglist.$(COMP): $(RPMS)
	rm -rf tmp && mkdir -p tmp/base
	ln -s $< tmp
	genpkglist --bloat tmp $(COMP)
	set +o posix && \
	zpkglist <tmp/base/$@* >$@
	rm -rf tmp
out2.lz4: pkglist.$(COMP) pkglist-query
	./pkglist-query '$(Q1)$(Q2)' $< | \
	sort -u |lz4 >$@
check: out1.lz4 out2.lz4
	lz4 -d <$< |grep -m1 bin/
	cmp $^
